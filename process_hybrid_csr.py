import os
import re
import time
import json
import requests
import pandas as pd
from geopy.geocoders import Nominatim, Photon
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

KNOWN_STATES = {
    "ANDAMAN & NICOBAR ISLANDS", "ANDHRA PRADESH", "ARUNACHAL PRADESH", "ASSAM", "BIHAR", 
    "CHANDIGARH", "CHHATTISGARH", "DADRA & NAGAR HAVELI AND DAMAN & DIU", "DELHI", "GOA", 
    "GUJARAT", "HARYANA", "HIMACHAL PRADESH", "JAMMU & KASHMIR", "JHARKHAND", "KARNATAKA", 
    "KERALA", "LADAKH", "MADHYA PRADESH", "MAHARASHTRA", "MANIPUR", "MEGHALAYA", "MIZORAM", 
    "NAGALAND", "ODISHA", "PUDUCHERRY", "PUNJAB", "RAJASTHAN", "SIKKIM", "TAMIL NADU", 
    "TELANGANA", "TRIPURA", "UTTAR PRADESH", "UTTARAKHAND", "WEST BENGAL", "ALL STATES", "ALL OVER INDIA", "INDIA"
}

def regex_fast_extract(raw_string):
    """
    Attempts to do a fast, deterministic extraction for simple patterns using Regex/Dictionaries.
    Returns structurally identical output to LLM if confident, else returns None.
    """
    raw_upper = str(raw_string).strip().upper()
    
    # Pattern 1: Just a known state or exactly "INDIA"
    if raw_upper in KNOWN_STATES:
        state_val = "India" if "ALL" in raw_upper or "INDIA" in raw_upper else raw_upper.title()
        return [{"state": state_val, "district": "", "city_town_village": ""}]
        
    # Pattern 2: Simple comma separated states (e.g., "MAHARASHTRA, GUJARAT")
    parts = [p.strip() for p in raw_upper.split(',')]
    if len(parts) > 1 and all(p in KNOWN_STATES for p in parts):
        return [{"state": p.title(), "district": "", "city_town_village": ""} for p in parts]
        
    # Pattern 3: Simple State (City/District) -> single brackets
    # e.g., "MAHARASHTRA (PUNE)"
    match = re.fullmatch(r'([A-Z\s&]+)\(([A-Z\s,-]+)\)', raw_upper)
    if match:
        state_part = match.group(1).strip()
        city_part = match.group(2).strip()
        if state_part in KNOWN_STATES:
            cities = [c.strip().title() for c in city_part.split(',') if c.strip()]
            return [{"state": state_part.title(), "district": "", "city_town_village": c} for c in cities]

    return None

def extract_addresses_with_llm(raw_string, model_name="llama3"):
    """
    Uses local Ollama to intelligently extract real-world locations.
    """
    if pd.isna(raw_string) or not str(raw_string).strip():
        return []

    prompt = f"""
    You are an expert Indian geography AI. Extract AND SEPARATE every single individual real-world location from the following 
    noisy string.
    Return ONLY a valid JSON object with a single key "locations" holding an array of dictionary objects.
    Each dictionary MUST have keys "state", "district", and "city_town_village". Leave empty if not applicable.

    Use English script and characters only as response.
    
    STRICT RULE: DO NOT combine multiple independent cities, districts, or villages into a single comma-separated string (e.g., 
    never output "Nagpur, Pune" in one field). You must create a SEPARATE dictionary for each independent place. ONLY group names 
    together if they represent a single hierarchical address tree (e.g., a specific village and its encompassing taluka), this should 
    checked first as a rule, only seperate when it doesn't make sense as a single address, use this rule for all seperation events.

    Example 1 (Multiple distinct districts):
    Input: "CHHATTISGARH (BILASPUR,DURG)"
    Output: {{"locations": [{{"state": "Chhattisgarh", "district": "Bilaspur", "city_town_village": ""}}, {{"state": "Chhattisgarh", "district": "Durg", "city_town_village": ""}}]}}
    
    Example 2 (Single address hierarchy):
    Input: "MAHARASHTRA(102A/102B MATOSHREE TOWER, BAI PADMABAI THAKKAR MARG, MAHIM, MUMBAI)" 
    Output: {{"locations": [{{"state": "Maharashtra", "district": "Mumbai", "city_town_village": "Mahim"}}]}}
    
    Example 3 (Single address hierarchy):
    Input: "GUJARAT(AJOL, TALUKA MANSA, GANDHINAGAR DIST.)" 
    Output: {{"locations": [{{"state": "Gujarat", "district": "Gandhinagar", "city_town_village": "Ajol, Mansa"}}]}}

    Example 4 (Multiple independent villages/towns):
    Input: "TELANGANA (KYASARUM, CHITKUL, BOLLARAM, PATANCHERU MANDAL)"
    Output: {{"locations": [{{"state": "Telangana", "district": "", "city_town_village": "Kyasarum"}}, {{"state": "Telangana", "district": "", "city_town_village": "Chitkul"}}, {{"state": "Telangana", "district": "", "city_town_village": "Bollaram"}}, {{"state": "Telangana", "district": "", "city_town_village": "Patancheru"}}]}}

    Target String:
    "{raw_string}"
    Output:
    """

    try:
        response = requests.post('http://localhost:11434/api/generate', json={
            "model": model_name,
            "prompt": prompt,
            "stream": False,
            "format": "json"
        }, timeout=60)
        
        if response.status_code == 200:
            text = response.json().get("response", "").strip()
            if not text:
                return []
            try:
                data = json.loads(text)
                if isinstance(data, dict) and "locations" in data:
                    return [item for item in data["locations"] if isinstance(item, dict)]
            except json.JSONDecodeError:
                pass
    except Exception as e:
        print(f"Local LLM Error for {raw_string}: {e}")
    
    return []

def hybrid_extract(raw_string):
    """
    Tandem approach: try fast deterministic regex/dict first.
    If it fails, fallback to LLM.
    """
    if pd.isna(raw_string) or not str(raw_string).strip():
        return []
    
    raw_str = str(raw_string).strip()
    
    # 1. Fast Dictionary / Regex Sweep
    fast_result = regex_fast_extract(raw_str)
    if fast_result:
        for res in fast_result: res['_extractor_src'] = 'regex'
        return fast_result
        
    # 2. LLM Sweep
    llm_result = extract_addresses_with_llm(raw_str)
    for res in llm_result: res['_extractor_src'] = 'llm'
    return llm_result

def geocode_location(query_dict, geolocator_primary, geolocator_fallback, retries=2):
    if not query_dict:
        return {"latitude": None, "longitude": None, "state": None, "district": None, "city_town_village": None}
        
    default_state = query_dict.get('state', '')
    default_district = query_dict.get('district', '')
    default_city = query_dict.get('city_town_village', '')

    clean_query = {}
    if default_state: clean_query['state'] = default_state
    if default_district: clean_query['county'] = default_district
    if default_city: clean_query['city'] = default_city
    clean_query['country'] = 'India'

    # Primary (Nominatim)
    time.sleep(1.05)
    for _ in range(retries):
        try:
            loc = geolocator_primary.geocode(clean_query, addressdetails=True, timeout=5)
            if loc and 'address' in loc.raw:
                addr = loc.raw['address']
                return {
                    "latitude": loc.latitude, "longitude": loc.longitude,
                    "state": addr.get('state', addr.get('region', default_state)) or default_state,
                    "district": addr.get('state_district', addr.get('county', default_district)) or default_district,
                    "city_town_village": addr.get('city', addr.get('town', addr.get('village', default_city))) or default_city,
                    "_geocoder_src": 'nominatim'
                }
            break
        except Exception:
            time.sleep(1)

    # Fallback (Photon)
    free_form_query = f"{default_city} {default_district} {default_state} India".strip()
    try:
        loc = geolocator_fallback.geocode(free_form_query, timeout=5)
        if loc:
            return {
                "latitude": loc.latitude, "longitude": loc.longitude,
                "state": default_state, "district": default_district, "city_town_village": default_city,
                "_geocoder_src": 'photon'
            }
    except Exception:
        pass

    return {
        "latitude": None, "longitude": None,
        "state": default_state, "district": default_district, "city_town_village": default_city,
        "_geocoder_src": 'failed'
    }

def process_hybrid(input_file, limit=200):
    start_time = time.time()
    #print(f"Hybrid Processing {input_file}... Limit set to {limit} rows.")
    df = pd.read_csv(input_file)
    
    # if limit:
    #     df = df.head(limit)
        
    geolocator_nom = Nominatim(user_agent="india_csr_hybrid_primary")
    geolocator_pho = Photon(user_agent="india_csr_hybrid_fallback")

    cache = {}
    geopy_cache = {}
    exploded_records = []

    for index, row in df.iterrows():
        raw_str = row.get('States (City/Town/District/Village)', None)
        base_record = row.to_dict()
        if 'States (City/Town/District/Village)' in base_record:
            del base_record['States (City/Town/District/Village)']

        if pd.isna(raw_str) or not str(raw_str).strip():
            exploded_records.append({**base_record, "latitude": None, "longitude": None, "state": None, "district": None, "city_town_village": None, "Original_Raw_Location": raw_str})
            continue

        raw_str = str(raw_str).strip()
        
        if raw_str not in cache:
            cache[raw_str] = hybrid_extract(raw_str)
            extractor = "REGEX" if cache[raw_str] and any(r.get('_extractor_src') == 'regex' for r in cache[raw_str]) else "LLM"
            print(f"[{extractor}] '{raw_str}' -> {len(cache[raw_str])} locations extracted.")
        
        queries = cache[raw_str]

        if not queries:
            exploded_records.append({**base_record, "latitude": None, "longitude": None, "state": None, "district": None, "city_town_village": None, "Original_Raw_Location": raw_str})
            continue

        for q_dict in queries:
            hash_dict = {k:v for k,v in q_dict.items() if not k.startswith('_')}
            dict_str = json.dumps(hash_dict, sort_keys=True)
            
            if dict_str not in geopy_cache:
                geopy_cache[dict_str] = geocode_location(hash_dict, geolocator_nom, geolocator_pho)
                geo = geopy_cache[dict_str]
                status = geo.get('_geocoder_src', 'failed')
                print(f"Geocoded [{status}]: {hash_dict} -> {geo.get('latitude')}, {geo.get('longitude')}")
                
            geo_info = geopy_cache[dict_str]
            row_out = base_record.copy()
            row_out["latitude"] = geo_info["latitude"]
            row_out["longitude"] = geo_info["longitude"]
            row_out["state"] = geo_info["state"]
            row_out["district"] = geo_info["district"]
            row_out["city_town_village"] = geo_info["city_town_village"]
            row_out["Original_Raw_Location"] = raw_str
            row_out["Extractor_Method"] = q_dict.get('_extractor_src', 'unknown')
            row_out["Geocoder_Method"] = geo_info.get('_geocoder_src', 'unknown')
            exploded_records.append(row_out)

    flattened_df = pd.DataFrame(exploded_records)
    out_file = input_file.replace('.csv', '_Hybrid_Classified_Batch.csv')
    flattened_df.to_csv(out_file, index=False)
    
    elapsed = time.time() - start_time
    print(f"Done! Saved {len(flattened_df)} total extracted rows to {out_file}")
    print(f"Time elapsed: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")

if __name__ == "__main__":
    process_hybrid("CSR_activities_2014-15.csv")#, limit=800)

