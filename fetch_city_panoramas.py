import os
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import argparse
from filelock import FileLock

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Fetch panoramic data for a city.")
parser.add_argument("--location", type=str, required=True, help="Name of the city to process")
args = parser.parse_args()
city = args.location  # Get city name from --location argument

# File paths
total_request_file = "total-request.json"
city_dashboard_file = "city-dashboard.json"
progress_file = "progress.json"
progress_file_lock = "progress.json.lock"  # Lock file for progress.json
output_dir = f"panoramic_coords/{city}"
os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist

# API Key
API_KEY = 'AIzaSyArVaE8IDQ2oXtbpTPkn_Z-cdByragcA9g' 

# Retryable session for HTTP requests
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Initialize total-request file if not present
def initialize_total_request_file():
    if not os.path.exists(total_request_file):
        with open(total_request_file, "w", encoding="utf-8") as file:
            json.dump({"total-request": 0}, file, indent=4)

# Update total-request count
def update_total_request_count():
    with open(total_request_file, "r+", encoding="utf-8") as file:
        data = json.load(file)
        data["total-request"] = data.get("total-request", 0) + 1
        file.seek(0)
        json.dump(data, file, indent=4)

# Save progress for a city with locking
def save_progress(city_name, lat, lng):
    with FileLock(progress_file_lock):  # Lock the file
        progress_data = {}
        if os.path.exists(progress_file):
            with open(progress_file, "r", encoding="utf-8") as file:
                try:
                    progress_data = json.load(file)
                except json.JSONDecodeError:
                    print("Error decoding progress file. Starting fresh.")
        
        # Update the progress for the specific city
        progress_data[city_name] = {"last_lat": lat, "last_lng": lng}

        # Save the progress back to the file
        with open(progress_file, "w", encoding="utf-8") as file:
            json.dump(progress_data, file, indent=4, ensure_ascii=False)

# Load progress for a city with locking
def load_progress(city_name):
    with FileLock(progress_file_lock):  # Lock the file
        if os.path.exists(progress_file):
            with open(progress_file, "r", encoding="utf-8") as file:
                try:
                    progress_data = json.load(file)
                    city_progress = progress_data.get(city_name, {})
                    return city_progress.get("last_lat"), city_progress.get("last_lng")
                except json.JSONDecodeError:
                    print("Error decoding progress file.")
        return None, None

# Get city coordinates from JSON file
def get_city_coordinates(city_name):
    with open(city_dashboard_file, "r", encoding="utf-8") as file:
        cities = json.load(file)
        for city in cities:
            if city["city"].lower() == city_name.lower():
                region = city.get("regions", [{}])[0]
                return (
                    float(region.get("min_latitude", city["min_latitude"])),
                    float(region.get("min_longitude", city["min_longitude"])),
                    float(region.get("max_latitude", city["max_latitude"])),
                    float(region.get("max_longitude", city["max_longitude"]))
                )
    raise ValueError(f"City '{city_name}' not found in {city_dashboard_file}.")

# Check if a coordinate is already saved
def is_saved_across_files(output_dir, base_name, lat, lng):
    key = f"{lat},{lng}"
    for file_name in os.listdir(output_dir):
        if file_name.startswith(base_name) and file_name.endswith(".json"):
            with open(os.path.join(output_dir, file_name), "r", encoding="utf-8") as file:
                try:
                    if key in json.load(file):
                        return True
                except json.JSONDecodeError:
                    print(f"Error decoding {file_name}. Skipping.")
    return False

# Fetch panorama metadata
def fetch_pano_metadata(lat, lng, radius=50):
    url = "https://maps.googleapis.com/maps/api/streetview/metadata"
    params = {"location": f"{lat},{lng}", "radius": radius, "key": API_KEY}
    
    try:
        response = session.get(url, params=params, timeout=10)
        update_total_request_count()
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "OK":
                return data.get("pano_id"), data.get("location"), data.get("date", "Unknown")
        else:
            print(f"Failed request for {lat},{lng}: {response.status_code} {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request error for {lat},{lng}: {e}")
    
    return None, None, None

def save_coord(base_name, lat, lng, pano_id, label, date, file_limit=500):
    key = f"{lat},{lng}"
    coord_data = {
        key: {
            "latitude": lat,
            "longitude": lng,
            "pano_id": pano_id,
            "address": label,
            "date": date
        }
    }

    # Collect existing files that match the base_name pattern.
    dir_name = os.path.dirname(base_name)
    file_prefix = os.path.basename(base_name)
    existing_files = []
    for f in os.listdir(dir_name):
        # Match base_name.json or base_name-<number>.json
        if f.startswith(file_prefix) and f.endswith(".json"):
            existing_files.append(f)

    # Sort files by their suffix number (no suffix = first file)
    # base_name.json comes first, then base_name-1.json, base_name-2.json, etc.
    def file_sort_key(fname):
        if fname == file_prefix + ".json":
            return 0
        else:
            # Extract the number after the dash
            parts = fname.split(".")[0].split("-")
            if len(parts) > 1 and parts[-1].isdigit():
                return int(parts[-1])
            return 999999  # If something doesn't match, put it at the end

    existing_files.sort(key=file_sort_key)

    # Determine the file to write to
    # - If no files, start with base_name.json
    # - If the last file is full, create a new one with next number
    if not existing_files:
        current_file = base_name + ".json"
        existing_data = {}
    else:
        # Check the last file
        last_file = existing_files[-1]
        current_file = os.path.join(dir_name, last_file)

        # Load existing data
        try:
            with open(current_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                if not isinstance(existing_data, dict):
                    print(f"Unexpected format in {current_file}. Starting fresh.")
                    existing_data = {}
        except (json.JSONDecodeError, FileNotFoundError):
            print(f"Error reading {current_file}. Starting fresh.")
            existing_data = {}

        # Check if the file hit the limit
        if len(existing_data) >= file_limit:
            # Need to create a new file with next number
            # Find the next suffix number
            if last_file == file_prefix + ".json":
                # first split file is -1
                new_suffix = 1
            else:
                parts = last_file.split(".")[0].split("-")
                if len(parts) > 1 and parts[-1].isdigit():
                    new_suffix = int(parts[-1]) + 1
                else:
                    new_suffix = 1

            current_file = os.path.join(dir_name, f"{file_prefix}-{new_suffix}.json")
            existing_data = {}

    print(f"saving {pano_id} pano to {current_file}")

    # Append or update entry
    existing_data.update(coord_data)

    # Write updated data
    with open(current_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)

    return current_file


# Fetch panoramas for a city
def fetch_city_panoramas(city_name, step=0.001):
    min_lat, min_lng, max_lat, max_lng = get_city_coordinates(city_name)
    base_name = os.path.join(output_dir, city_name.lower() + "_panoramic_coords")

    # Resume from the last progress
    last_lat, last_lng = load_progress(city_name)
    current_lat = last_lat if last_lat else min_lat
    current_lng = last_lng if last_lng else min_lng

    while current_lat <= max_lat:
        while current_lng <= max_lng:
            if not is_saved_across_files(output_dir, os.path.basename(base_name), current_lat, current_lng):
                pano_id, location, date = fetch_pano_metadata(current_lat, current_lng)
                if pano_id:
                    save_coord(base_name, current_lat, current_lng, pano_id, "Unknown", date)
                save_progress(city_name, current_lat, current_lng)
            current_lng += step
        current_lat += step
        current_lng = min_lng
        time.sleep(0.2)

# Initialize and run
initialize_total_request_file()
fetch_city_panoramas(city)
