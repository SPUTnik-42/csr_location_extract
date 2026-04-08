import os
import re
import time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def ensure_dirs():
    """Ensure output directories exist."""
    for d in ['location', 'location_geopy', 'flattened', 'original']:
        os.makedirs(d, exist_ok=True)

def extract_year_label(file_path):
    """Extract year structure from filename, e.g., 'CSR_activities_2014-15.xlsx' -> '2014-15'"""
    base_name = os.path.basename(file_path)
    match = re.search(r'(\d{4}-\d{2})', base_name)
    if match:
        return match.group(1)
    # Fallback to the whole filename without extension
    return os.path.splitext(base_name)[0]

def step1_extract_and_save_locations(input_file, year_label):
    """
    Equivalent to Sections 1-4.
    Reads the data, extracts the locations via regex, and saves to the `location/` folder.
    """
    print(f"\n--- STEP 1: Extraction for {year_label} ---")
    if input_file.endswith('.xlsx'):
        df = pd.read_excel('original/'  + input_file)
    else:
        df = pd.read_csv(input_file)
        
    location_classification = {}
    
    def extract_locations(loc_str):
        if pd.isna(loc_str) or not isinstance(loc_str, str):
            return
            
        loc_str = loc_str.strip()
        matches = re.finditer(r'([^\(]+)(?:\(([^)]+)\))?', loc_str)
        
        for match in matches:
            state_part = match.group(1)
            details_part = match.group(2)
            
            if state_part:
                clean_state_part = state_part.strip(' ,')
                for s in re.split(r'[/,;]', clean_state_part):
                    s = s.strip(' \t\n\r-|')
                    if s:
                        location_classification[s] = 'State'
                        
            if details_part:
                parts = [p.strip(' \t\n\r-|') for p in re.split(r'[,;&]', details_part) if p.strip()]
                for part in parts:
                    if part:
                        if part not in location_classification:
                            location_classification[part] = 'Unclassified'

    if 'States (City/Town/District/Village)' in df.columns:
        df['States (City/Town/District/Village)'].apply(extract_locations)
        records = [{'Location': loc, 'Class': cls} for loc, cls in location_classification.items()]
        locations_df = pd.DataFrame(records)
        locations_df = locations_df.sort_values(by=['Class', 'Location']).reset_index(drop=True)
        
        out_path = os.path.join('location', f'Location_Classification_{year_label}.csv')
        locations_df.to_csv(out_path, index=False)
        print(f"Extracted {len(locations_df)} unique locations to {out_path}")
        return df, locations_df
    else:
        print("Required column not found.")
        return df, pd.DataFrame()

def step2_geopy_classification(extracted_locations_df, mirror_path, year_label):
    """
    Equivalent to Sections 5-6.
    Checks the global master mirror dataset. Only queries Geopy for NEW locations.
    Saves the snapshot for this year to `location_geopy/` and updates the master mirror.
    """
    print(f"\n--- STEP 2: Geopy Classification for {year_label} ---")
    
    # Load Master Mirror if exists
    if os.path.exists(mirror_path):
        mirror_df = pd.read_csv(mirror_path)
        master_dict = dict(zip(mirror_df['Location'].str.strip(), mirror_df['Intelligent_Class']))
        print(f"Loaded master mirror dataset with {len(master_dict)} mapped locations.")
    else:
        master_dict = {}
        print("No master mirror dataset found. Will create one.")

    known_states_uts = {
        "ANDAMAN & NICOBAR ISLANDS", "ANDHRA PRADESH", "ARUNACHAL PRADESH", "ASSAM", "BIHAR", 
        "CHANDIGARH", "CHHATTISGARH", "DADRA & NAGAR HAVELI AND DAMAN & DIU", "DELHI", "GOA", 
        "GUJARAT", "HARYANA", "HIMACHAL PRADESH", "JAMMU & KASHMIR", "JHARKHAND", "KARNATAKA", 
        "KERALA", "LADAKH", "MADHYA PRADESH", "MAHARASHTRA", "MANIPUR", "MEGHALAYA", "MIZORAM", 
        "NAGALAND", "ODISHA", "PUDUCHERRY", "PUNJAB", "RAJASTHAN", "SIKKIM", "TAMIL NADU", 
        "TELANGANA", "TRIPURA", "UTTAR PRADESH", "UTTARAKHAND", "WEST BENGAL"
    }

    geolocator = Nominatim(user_agent="india_csr_classifier_script")
    
    intelligent_classes = []
    new_queries = 0
    
    for _, row in extracted_locations_df.iterrows():
        loc = str(row['Location']).strip()
        
        # 1. If it's already mapped globally, use it
        if loc in master_dict:
            intelligent_classes.append(master_dict[loc])
            continue
            
        # 2. Hardcoded state checking
        if loc.upper() in known_states_uts:
            intelligent_classes.append("State")
            master_dict[loc] = "State"
            continue
            
        # 3. Simple Keywords Override (pre-API to save calls)
        loc_upper = loc.upper()
        if 'DIST.' in loc_upper or 'DISTRICT' in loc_upper:
            cls = 'District'
        elif 'VILLAGE' in loc_upper:
            cls = 'Village'
        elif 'CITY' in loc_upper:
            cls = 'City'
        elif 'TOWN' in loc_upper:
            cls = 'Town'
        else:
            # 4. Geopy API Fallback
            new_queries += 1
            time.sleep(1.05) # Rate limit
            try:
                query = f"{loc}, India"
                res = geolocator.geocode(query, addressdetails=True, timeout=10)
                if res and hasattr(res, 'raw') and 'addresstype' in res.raw:
                    ctype = res.raw['addresstype'].lower()
                    if ctype in ['state', 'union_territory', 'region']: cls = "State"
                    elif ctype in ['state_district', 'district', 'county', 'borough']: cls = "District"
                    elif ctype in ['city', 'municipality', 'city_district']: cls = "City"
                    elif ctype in ['town']: cls = "Town"
                    elif ctype in ['village', 'hamlet']: cls = "Village"
                    else: cls = "Town"
                else:
                    cls = "Not Found"
            except Exception:
                cls = "Not Found"
                
        intelligent_classes.append(cls)
        master_dict[loc] = cls
        
    print(f"Executed Geopy queries for {new_queries} new locations.")
    
    # Save this year's specific geocoded view
    extracted_locations_df['Intelligent_Class'] = intelligent_classes
    year_out_path = os.path.join('location_geopy', f'Location_Geopy_{year_label}.csv')
    extracted_locations_df.to_csv(year_out_path, index=False)
    
    # Write back the updated master mirror dataset
    updated_master = pd.DataFrame([{'Location': k, 'Intelligent_Class': v} for k, v in master_dict.items()])
    # Pre-merge Class if possible, but minimal required is Location & Intelligent_Class
    updated_master.to_csv(mirror_path, index=False)
    
    print(f"Saved {year_label} geopy output to {year_out_path}")
    print(f"Updated Master Mirror dataset at {mirror_path}")
    
    return updated_master

def step3_heuristics_and_flatten(raw_df, updated_master_df, mirror_path, year_label):
    """
    Equivalent to Sections 7-8.
    Applies heuristics to 'Not Found' in the master mirror.
    Then unravels the original dataframe into explicit rows.
    Saves to `flattened/`.
    """
    print(f"\n--- STEP 3: Heuristics & Flattening for {year_label} ---")
    
    village_keywords = ['GAON', 'WADI', 'PALLY', 'HALLI', 'KUPPAM', 'PADA', 'GUDA', 'GRAM', 'PANCHAYAT', 'THANDA', 'CHERRY', 'VILLAGE', 'KHERA', 'PURAM']
    town_keywords = ['NAGAR', 'PUR', 'ABAD', 'BAGH', 'VIHAR', 'KHAND', 'PET', 'TALUKA', 'TEHSIL', 'MANDAL', 'CITY', 'TOWN', 'URBAN', 'KOTA', 'NAGRI', 'COLONY', 'ESTATE']
    landmark_keywords = ['SCHOOL', 'HOSPITAL', 'CLINIC', 'ROAD', 'MARG', 'CROSSING', 'AREA', 'SLUM', 'VALLEY', 'PLANT', 'MINES', 'MACHINE']

    def apply_heuristics(row):
        loc = str(row['Location']).upper()
        cls = row['Intelligent_Class']
        
        if cls != 'Not Found':
            return cls
        if any(kw in loc for kw in village_keywords): return 'Village'
        if any(kw in loc for kw in town_keywords): return 'Town'
        if any(kw in loc for kw in landmark_keywords): return 'Town'
        if '&' in loc: return 'Town'
        return 'Town' # Default failsafe
        
    master_before_nf = (updated_master_df['Intelligent_Class'] == 'Not Found').sum()
    updated_master_df['Intelligent_Class'] = updated_master_df.apply(apply_heuristics, axis=1)
    master_after_nf = (updated_master_df['Intelligent_Class'] == 'Not Found').sum()
    
    if master_before_nf != master_after_nf:
        updated_master_df.to_csv(mirror_path, index=False)
        print(f"Applied Heuristics globally. 'Not Found' reduced from {master_before_nf} to {master_after_nf}.")
        print("Master mirror updated.")

    loc_to_class = dict(zip(updated_master_df['Location'].str.strip(), updated_master_df['Intelligent_Class']))
    
    exploded_records = []
    
    for idx, row in raw_df.iterrows():
        loc_str = row.get('States (City/Town/District/Village)', None)
        base_record = row.to_dict()
        if 'States (City/Town/District/Village)' in base_record:
            del base_record['States (City/Town/District/Village)']
            
        if pd.isna(loc_str) or not isinstance(loc_str, str):
            rec = base_record.copy()
            rec['Resolved_State'] = None
            rec['Resolved_Location'] = None
            rec['Location_Class'] = None
            exploded_records.append(rec)
            continue
            
        loc_str = loc_str.strip()
        matches = re.finditer(r'([^\(]+)(?:\(([^)]+)\))?', loc_str)
        found_any = False
        
        for match in matches:
            state_part = match.group(1)
            details_part = match.group(2)
            
            states = []
            if state_part:
                for s in re.split(r'[/,;]', state_part.strip(' ,')):
                    s = s.strip(' \t\n\r-|')
                    if s: states.append(s)
            
            sub_locs = []
            if details_part:
                for p in re.split(r'[,;&]', details_part):
                    p = p.strip(' \t\n\r-|')
                    if p: sub_locs.append(p)
            
            if states and sub_locs:
                for s in states:
                    for sl in sub_locs:
                        rec = base_record.copy()
                        rec['Resolved_State'] = s
                        rec['Resolved_Location'] = sl
                        rec['Location_Class'] = loc_to_class.get(sl, "Not Found")
                        exploded_records.append(rec)
                        found_any = True
            elif states and not sub_locs:
                for s in states:
                    rec = base_record.copy()
                    rec['Resolved_State'] = s
                    rec['Resolved_Location'] = s
                    rec['Location_Class'] = loc_to_class.get(s, "State")
                    exploded_records.append(rec)
                    found_any = True
            elif sub_locs and not states:
                for sl in sub_locs:
                    rec = base_record.copy()
                    rec['Resolved_State'] = "Unknown"
                    rec['Resolved_Location'] = sl
                    rec['Location_Class'] = loc_to_class.get(sl, "Not Found")
                    exploded_records.append(rec)
                    found_any = True
                    
        if not found_any:
            rec = base_record.copy()
            rec['Resolved_State'] = None
            rec['Resolved_Location'] = loc_str
            rec['Location_Class'] = "Not Found"
            exploded_records.append(rec)

    flattened_df = pd.DataFrame(exploded_records)
    flat_out_path = os.path.join('flattened', f'CSR_activities_Unravelled_{year_label}.csv')
    flattened_df.to_csv(flat_out_path, index=False)
    
    print(f"Flattening complete! Saved {len(flattened_df)} records to {flat_out_path}")
    return flattened_df

def process_csr_file(input_file, master_mirror_path='Location_Classification_Mirror.csv'):
    """
    Main orchestrator function coordinating all 3 steps.
    """
    try:
        ensure_dirs()
        year_label = extract_year_label(input_file)
        print(f"\n{'='*50}\nStarting Processing for {input_file} (Label: {year_label})\n{'='*50}")
        
        # Step 1
        raw_df, extracted_locs_df = step1_extract_and_save_locations(input_file, year_label)
        if extracted_locs_df.empty: return
        
        # Step 2
        updated_master = step2_geopy_classification(extracted_locs_df, master_mirror_path, year_label)
        
        # Step 3
        step3_heuristics_and_flatten(raw_df, updated_master, master_mirror_path, year_label)
        
        print(f"\nProcessing for {year_label} completed successfully!\n")
    except Exception as e:
        print(f"An error occurred while processing {input_file}: {e}")

if __name__ == "__main__":
    # Example Usage:
    # process_csr_file("CSR_activities_2014-15.xlsx")
    # process_csr_file("CSR_activities_2015-16.csv")
    print("Script ready. Import and call process_csr_file(filename) to use.")
