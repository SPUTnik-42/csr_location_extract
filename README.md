# CSR Data Location Extraction and Classification

This repository contains scripts and notebooks designed to process Indian Corporate Social Responsibility (CSR) datasets. The primary goal is to extract, classify, and normalize complex location strings into established geographical hierarchies (State, District, City, Town, Village) and flatten the dataset for easier analysis.

## Overview

The original CSR datasets contain a loosely structured column named `States (City/Town/District/Village)`, which often contains multiple states and sub-locations grouped together in formats like `MAHARASHTRA (MUMBAI, BOISAR)`, or standalone states like `GUJARAT`.

The code processes this data through a 3-step pipeline to:
1. Parse and extract individual location names using regex.
2. Intelligently classify each location using a mix of rule-based logic and the Geopy (Nominatim) API.
3. Unravel (flatten) the original dataset, creating individual rows for each specific state/sub-location pair, augmented with their computed geographical classification.

## File Structure

*   `csr_eda_geocoding.ipynb`: Initial exploratory data analysis and development of the classification logic for the 2014-15 dataset.
*   `process_csr_data.py`: The core Python script encapsulating the pipeline into reusable functions (`ensure_dirs`, `extract_year_label`, `step1_extract_and_save_locations`, `step2_geopy_classification`, `step3_heuristics_and_flatten`).
*   `process_yearly_csr.ipynb`: A demonstration notebook applying the script to year-over-year data (e.g., 2015-16, 2016-17) sequentially while sharing a persistent classification memory ("Master Mirror").
*   `Location_Classification_Mirror.csv`: The global "Master Dictionary" maintaining learned classifications to minimize redundant API calls.

## Step-by-Step Explanation and Decisions

### Step 1: Location Extraction

The first step attempts to disentangle the messy `States (City/Town/District/Village)` column.

*   **Decision:** A Regular Expression (`([^\(]+)(?:\(([^)]+)\))?`) is used to separate text outside of parentheses from text inside parentheses.
*   **Logic:**
    *   Text **outside** the brackets is treated as high-level locations (States) and mapped directly to the `State` category. Multiple states separated by commas or slashes are split into individual entities.
    *   Text **inside** the brackets is treated as specific sub-locations. These are split by commas or ampersands. Since their rank is unknown initially, they are temporarily flagged as `Unclassified`.
*   **Output:** Generates a raw list of unique locations found in the dataset for the current year.

### Step 2: Intelligent Geopy Classification

This step assigns a robust geographical category to the extracted locations.

*   **Decision - Utilizing a Master Map:** To avoid redundant (and slow) network calls, the script first checks a global master dictionary (`Location_Classification_Mirror.csv`). If a location was classified in a previous year (e.g., 2014-15), it instantly returns that classification for 2015-16.
*   **Decision - Hardcoded States:** A fixed list of known Indian States and Union Territories is used to quickly bypass API checks for standard states.
*   **Decision - Fast-path Keywords:** Before hitting the API, the code looks for obvious clues in the string itself (e.g., if it contains "DIST.", it's mapped to `District`; "CITY" to `City`).
*   **The Geopy API (Nominatim):** For locations that are genuinely new to the system, the script queries OpenStreetMap via Geopy.
    *   **Rate Limiting:** A `time.sleep(1.05)` is enforced to strictly adhere to Nominatim's free usage policy (maximum 1 request per second).
    *   **Mapping OS Type to Custom Classes:** The API returns an `addresstype` which is mapped to our required classes:
        *   `state`, `union_territory`, `region` -> `State`
        *   `state_district`, `district`, `county` -> `District`
        *   `city`, `municipality` -> `City`
        *   `town` -> `Town`
        *   `village`, `hamlet` -> `Village`
    *   **Default Fallback for API Hits:** If Nominatim returns an unknown type not covered above, it defaults to `Town`. If the API completely fails to find the location, it is tagged as `Not Found`.

### Step 3: Heuristics and Dataset Flattening

The final phase addresses unresolved API lookups and restructures the original dataset.

*   **Heuristics for "Not Found":** Locations that Geopy couldn't identify undergo a rule-based check looking for common Indian geographical suffixes:
    *   **Village Indicators:** `GAON`, `WADI`, `PALLY`, `HALLI`, `KUPPAM`, `GRAM`, etc. turn the classification into `Village`.
    *   **Town Indicators:** `NAGAR`, `PUR`, `ABAD`, `PET`, `MANDAL`, etc. turn it into `Town`.
    *   **Landmarks:** Mentions of `ROAD`, `SCHOOL`, `HOSPITAL`, or an ampersand (`&`) resolve to `Town`.
*   **Default Failsafe:** If all heuristics fail, the location is arbitrarily categorized as `Town` to ensure completeness, assuming most unrecognized industrial/CSR sites are smaller clusters or localities.
*   **Flattening (Unravelling):** The script iterates through the original dataset rows. It applies the regex logic again, but this time it associates the discovered states and sub-locations using a Cartesian product approach.
    *   If a row has "MAHARASHTRA (MUMBAI, PUNE)", it creates two new rows:
        1. Row 1: State = MAHARASHTRA, Location = MUMBAI, Class = City
        2. Row 2: State = MAHARASHTRA, Location = PUNE, Class = City
    *   If it only has a sub-location and no state, State becomes "Unknown".
    *   If parsing fails completely, the raw string is retained under Location, with Class "Not Found".

## Geographical Categories Used

The system standardizes all geographic references into the following strict taxonomy:

1.  **`State`**: The top-level administrative division (including Union Territories).
2.  **`District`**: The secondary administrative division within a state.
3.  **`City`**: Large urban agglomerations and major metropolitan areas.
4.  **`Town`**: Mid-sized urban centers, municipalities, or prominent localities. This also acts as the **default fallback category** for various infrastructure, landmarks, or completely unidentifiable strings that pass through the failsafe heuristics.
5.  **`Village`**: Rural settlements, hamlets, or gram panchayats.
6.  **`Unclassified`**: A temporary label used only during Step 1 for parsed strings before they hit the API.
7.  **`Not Found`**: Indicates the location string could not be identified by the Geopy API and did not match any fallback heuristic patterns. (Ideally rare after Step 3).
