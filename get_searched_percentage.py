import argparse
import json
import os
import glob

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process panoramic coordinates for a city.")
parser.add_argument("--location", type=str, required=True, help="City name for processing")
args = parser.parse_args()
city_name = args.location  # Get city name from --location argument

progress_file = "progress.json"
city_dashboard_file = "city-dashboard.json"


def get_last_created_file(directory):
    """Get the most recently created file in a directory."""
    files = glob.glob(os.path.join(directory, "*.json"))
    if not files:
        return None  # No JSON files in the directory
    # Sort files by creation time
    last_created_file = max(files, key=os.path.getctime)
    return last_created_file


# Ensure the output directory exists
directory = f"panoramic_coords/{city_name}"
os.makedirs(directory, exist_ok=True)

# Load or initialize output JSON file
last_file = get_last_created_file(directory)
if last_file:
    print(f"Last created file: {last_file}")
    output_json = last_file
else:
    print(f"No JSON files found in {directory}. Starting fresh.")
    output_json = os.path.join(directory, f"{city_name}_panoramic_coords-1.json")


# Load saved coordinates
saved_coords = {}
if os.path.exists(output_json):
    with open(output_json, "r", encoding="utf-8") as file:
        saved_coords = json.load(file)

# Count the number of keys
key_count = len(saved_coords.keys())
key_set_count = len(set(saved_coords.keys()))
print(f"Total img / img set: {key_count}/{key_set_count}")


def load_progress(city_name):
    """Load progress for the given city from the progress file."""
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as file:
            try:
                progress_data = json.load(file)
                city_progress = progress_data.get(city_name, {})
                return city_progress.get("last_lat"), city_progress.get("last_lng")
            except json.JSONDecodeError:
                print("Error decoding progress file.")
    return None, None


def save_progress(city_name, lat, lng):
    """Save progress for the given city to the progress file."""
    progress_data = {}
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as file:
            try:
                progress_data = json.load(file)
            except json.JSONDecodeError:
                print("Error decoding progress file. Starting fresh.")

    # Update or add progress for the city
    progress_data[city_name] = {"last_lat": lat, "last_lng": lng}

    with open(progress_file, "w", encoding="utf-8") as file:
        json.dump(progress_data, file, indent=4, ensure_ascii=False)


def get_city_coordinates(city_name):
    """Get the latitude and longitude boundaries for a city."""
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


def calculate_progress(step):
    """Calculate the progress of the panorama retrieval."""
    current_lat, current_lng = load_progress(city_name)
    if current_lat is None or current_lng is None:
        print("No progress found, starting from scratch.")
        return 0.0

    min_lat, min_lng, max_lat, max_lng = get_city_coordinates(city_name)

    total_steps_lat = (max_lat - min_lat) / step
    total_steps_lng = (max_lng - min_lng) / step
    total_steps = total_steps_lat * total_steps_lng

    completed_steps_lat = (current_lat - min_lat) / step
    completed_steps_lng = (current_lng - min_lng) / step
    completed_steps = completed_steps_lat * total_steps_lng + completed_steps_lng

    progress_percentage = (completed_steps / total_steps) * 100
    return progress_percentage


# Example usage
progress = calculate_progress(0.001)
print(f"Progress: {progress:.2f}%")
