import os
import time
import json
import requests
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

def extract_addresses_with_llm(raw_string, model_name="llama3"):
    """
    Uses local Ollama to intelligently extract real-world locations 
    from ambiguous strings.
    """
    if pd.isna(raw_string) or not str(raw_string).strip():
        return []

    # Better prompt specifying a JSON object instead of raw array (which models often mess up)
    prompt = f"""
    You are an expert Indian geography AI. Extract AND SEPARATE every single individual real-world location from the following noisy string.
    Return ONLY a valid JSON object with a single key "locations" holding an array of dictionary objects.
    Each dictionary MUST have keys "state", "district", and "city_town_village". Leave empty if not applicable.
    
    Example 1:
    Input: "CHHATTISGARH (BILASPUR,DURG)"
    Output: {{"locations": [{{"state": "Chhattisgarh", "district": "Bilaspur", "city_town_village": ""}}, {{"state": "Chhattisgarh", "district": "Durg", "city_town_village": ""}}]}}
    
    Example 2:
    Input: "MAHARASHTRA(102A/102B MATOSHREE TOWER@@@BAI PADMABAI THAKKAR MARG@@@MAHIM@@@MUMBAI)" 
    Output: {{"locations": [{{"state": "Maharashtra", "district": "Mumbai", "city_town_village": "Mahim"}}]}}
    
    Example 3:
    Input: "GUJARAT(AJOL@@@TALUKA MANSA@@@GANDHINAGAR DIST.)" 
    Output: {{"locations": [{{"state": "Gujarat", "district": "Gandhinagar", "city_town_village": "Ajol, Mansa"}}]}}

    Example 4:
    Input: "ALL STATES"
    Output: {{"locations": [{{"state": "India", "district": "", "city_town_village": ""}}]}}

    Target String:
    "{raw_string}"
    Output:
    """

    try:
        response = requests.post('http://localhost:11434/api/generate', json={
            "model": model_name,
            "prompt": prompt,
            "stream": False,
            "format": "json" # forces local model into JSON mode
        }, timeout=60)
        
        if response.status_code == 200:
            text = response.json().get("response", "").strip()
            if not text:
                return []
            
            try:
                data = json.loads(text)
                if isinstance(data, dict) and "locations" in data:
                    return [item for item in data["locations"] if isinstance(item, dict)]
                elif isinstance(data, list):
                    return [item for item in data if isinstance(item, dict)]
            except json.JSONDecodeError:
                print(f"JSON Parse Error for {raw_string}. LLM Output: {text}")
    except Exception as e:
        print(f"Local LLM Error for {raw_string}: {e}")
    
    return []

def geocode_location(query_dict, geolocator, retries=3):
    """
    Geocodes the location to extract structured geographic features.
    """
    if not query_dict:
        return {"latitude": None, "longitude": None, "state": None, "district": None, "city_town_village": None}
        
    # Provide defaults to prevent entirely empty fields
    default_state = query_dict.get('state', '')
    default_district = query_dict.get('district', '')
    default_city = query_dict.get('city_town_village', '')

    # Nominatim handles structured query mapping well for mapping keys appropriately
    # We rename 'city_town_village' back to standard 'city' for Nominatim
    clean_query = {}
    if query_dict.get('state'): clean_query['state'] = query_dict['state']
    if query_dict.get('district') or query_dict.get('county'): clean_query['county'] = query_dict.get('district')
    if query_dict.get('city_town_village'): clean_query['city'] = query_dict.get('city_town_village')
    
    # Add country for better hit rate
    clean_query['country'] = 'India'

    time.sleep(1.05) # Rate limiting for Nominatim
    for attempt in range(retries):
        try:
            # We map using Structured Search explicitly setting dictionary format query!
            location = geolocator.geocode(clean_query, addressdetails=True, timeout=10)
            if location and hasattr(location, 'raw') and 'address' in location.raw:
                address = location.raw['address']
                return {
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    "state": address.get('state', address.get('region', default_state)) or default_state,
                    "district": address.get('state_district', address.get('county', default_district)) or default_district,
                    "city_town_village": address.get('city', address.get('town', address.get('village', address.get('suburb', default_city)))) or default_city
                }
            break # If successful but no address details, break early
        except (GeocoderTimedOut, GeocoderUnavailable):
            if attempt < retries - 1:
                time.sleep(2)
                continue
            print(f"Geocoding timeout for {clean_query}")
        except Exception as e:
            print(f"Geocoding Error for {clean_query}: {e}")
            break
            
    # Fallback to LLM entries exactly as predicted if Geocoder fails!
    return {
        "latitude": None, 
        "longitude": None, 
        "state": default_state, 
        "district": default_district, 
        "city_town_village": default_city
    }

def process_file_with_ai(input_file, limit=200):
    start_time = time.time()
    print(f"Processing {input_file}... Limit set to {limit} rows.")
    df = pd.read_csv(input_file)
    
    if limit:
        df = df.head(limit)
        
    geolocator = Nominatim(user_agent="india_csr_ai_classifier_ver2")

    llm_cache = {}
    geopy_cache = {}

    exploded_records = []

    for index, row in df.iterrows():
        raw_str = row.get('States (City/Town/District/Village)', None)
        base_record = row.to_dict()
        if 'States (City/Town/District/Village)' in base_record:
            del base_record['States (City/Town/District/Village)']

        if pd.isna(raw_str) or not str(raw_str).strip():
            exploded_records.append({**base_record, "latitude": None, "longitude": None, "state": None, "district": None, "city_town_village": None, "Original_Raw_Location": raw_str, "LLM_Cleaned_Query": None})
            continue

        raw_str = str(raw_str).strip()
        
        # 1. Ask LLM to extract standardized queries
        if raw_str not in llm_cache:
            llm_cache[raw_str] = extract_addresses_with_llm(raw_str)
            print(f"LLM mapped: '{raw_str}' -> {llm_cache[raw_str]}")
        
        queries = llm_cache[raw_str]

        # If LLM failed to extract anything, fallback
        if not queries:
            print(f"FAILED to extract locations from: {raw_str}")
            exploded_records.append({**base_record, "latitude": None, "longitude": None, "state": None, "district": None, "city_town_village": None, "Original_Raw_Location": raw_str, "LLM_Cleaned_Query": None})
            continue

        # 2. Ask Geopy to get detailed coordinates for each extracted query
        for q_dict in queries:
            # Make dict hashable to use as cache key by converting it to string
            dict_str = json.dumps(q_dict, sort_keys=True)
            if dict_str not in geopy_cache:
                geopy_cache[dict_str] = geocode_location(q_dict, geolocator)
                geo = geopy_cache[dict_str]
                status = "SUCCESS" if geo['latitude'] else "FAILED"
                print(f"Geocoded [{status}]: {q_dict} -> {geo['state']} | {geo['district']} | {geo['city_town_village']}")
                
            geo_info = geopy_cache[dict_str]
            row_out = base_record.copy()
            row_out["latitude"] = geo_info["latitude"]
            row_out["longitude"] = geo_info["longitude"]
            row_out["state"] = geo_info["state"]
            row_out["district"] = geo_info["district"]
            row_out["city_town_village"] = geo_info["city_town_village"]
            row_out["Original_Raw_Location"] = raw_str
            row_out["LLM_Cleaned_Query"] = dict_str
            exploded_records.append(row_out)

        # Save progress occasionally
        if index > 0 and index % 10 == 0:
            print(f"Processed {index} rows...")

    flattened_df = pd.DataFrame(exploded_records)
    out_file = input_file.replace('.csv', '_AI_Classified_Batch.csv')
    flattened_df.to_csv(out_file, index=False)
    
    elapsed = time.time() - start_time
    print(f"Done! Saved test batch dataset to {out_file}")
    print(f"Time elapsed: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")

if __name__ == "__main__":
    process_file_with_ai("CSR_activities_2014-15.csv")#, limit=200)
