import pandas as pd
import re
from pyproj import Proj, Transformer
from rapidfuzz import process, fuzz, utils

# Compile regex pattern for extracting prefixes and platform codes
def compile_regex():
    # Matches sequences of Unicode word characters, spaces, and parentheses followed by "P" and digits
    return re.compile(r'([\w\s\(\)\-]*)P(\d+)', re.UNICODE)

# Load the GTFS stops CSV file into a DataFrame
input_file = 'stops.txt'  # Replace with your actual file path
df = pd.read_csv(input_file, dtype=str)  # Ensure stop_id is read as a string

# Compile the regex pattern
pattern = compile_regex()

# Extract the matched patterns from stop names
df['stop_name_normalized'] = df['stop_name'].apply(lambda x: pattern.findall(str(x)))
df['prefix'] = df['stop_name_normalized'].apply(lambda x: x[0][0].strip() if x else "")  # Extract prefix and strip spaces
df['platform_code'] = df['stop_name_normalized'].apply(lambda x: x[0][1].strip() if x else "")  # Extract digits and strip spaces

# Filter rows that matched the regex (non-empty prefix)
df_matches = df[df['prefix'] != ""].copy()

# Fuzzy group similar prefixes
prefixes = df_matches['prefix'].unique()
grouped_prefixes = {}

# Iterate through each prefix and group similar ones using fuzzy matching
for prefix in prefixes:
    # Find best matches within the unique prefixes
    matches = process.extract(prefix, prefixes, scorer=fuzz.ratio, limit=None, processor=utils.default_process)
    # Group matches with a score above a threshold (e.g., 80)
    similar_prefixes = [match[0] for match in matches if match[1] >= 90]
    group_key = min(similar_prefixes, key=len)  # Choose the shortest match as the key
    if group_key not in grouped_prefixes:
        grouped_prefixes[group_key] = set()
    grouped_prefixes[group_key].update(similar_prefixes)

# Map each stop to its grouped prefix key
df_matches['grouped_prefix'] = df_matches['prefix'].apply(
    lambda x: next((key for key, vals in grouped_prefixes.items() if x in vals), x)
)

# Group by the fuzzily matched prefixes
grouped = df_matches.groupby('grouped_prefix')

# Define the UTM zone 29N projection (WGS84, northern hemisphere)
utm_proj = Proj(proj="utm", zone=29, ellps="WGS84", hemisphere='north')
geo_proj = Proj(proj="latlong", datum="WGS84")
transformer_to_utm = Transformer.from_proj(geo_proj, utm_proj, always_xy=True)
transformer_to_geo = Transformer.from_proj(utm_proj, geo_proj, always_xy=True)

def geographic_centroid_utm(latitudes, longitudes):
    """
    Calculate the geographic centroid using UTM coordinates.
    """
    # Convert latitude and longitude to UTM coordinates
    utm_coords = [transformer_to_utm.transform(lon, lat) for lat, lon in zip(latitudes, longitudes)]
    x_coords, y_coords = zip(*utm_coords)

    # Calculate average UTM coordinates
    x_mean = sum(x_coords) / len(x_coords)
    y_mean = sum(y_coords) / len(y_coords)

    # Convert the average UTM coordinates back to latitude and longitude
    lon_centroid, lat_centroid = transformer_to_geo.transform(x_mean, y_mean)
    return lat_centroid, lon_centroid

# Calculate geographic center and create parent stations with incrementing IDs
parent_stations = []
parent_station_counter = 1

for prefix, group in grouped:
    if len(group) <= 1:
        continue
    # Calculate the geographic centroid using UTM zone 29N
    mean_lat, mean_lon = geographic_centroid_utm(group['stop_lat'], group['stop_lon'])

    # Create a new parent station entry with incrementing ID PS1, PS2, ...
    parent_station_id = f"PS{parent_station_counter}"
    parent_station_counter += 1

    parent_station = {
        'stop_id': parent_station_id,
        'stop_name': f"{prefix}",
        'stop_lat': mean_lat,
        'stop_lon': mean_lon,
        'location_type': 1,  # Set location_type to 1 to indicate it’s a station
        'parent_station': '',  # Ensure no parent_station for the parent station itself
    }
    parent_stations.append(parent_station)

    # Update each stop in the group to reference the new parent station and set the platform code
    df.loc[group.index, 'parent_station'] = parent_station_id

# Create a DataFrame for the parent stations
parent_stations_df = pd.DataFrame(parent_stations, dtype=str)

# Concatenate the parent stations to the main DataFrame
df = pd.concat([df, parent_stations_df], ignore_index=True)
df.fillna('', inplace=True)

# Drop auxiliary columns
df = df.drop('stop_name_normalized', axis=1)
df = df.drop('prefix', axis=1)

# Save the modified DataFrame back to a CSV file
output_file = 'modified_stops.txt'
df.to_csv(output_file, index=False)

print(f"Modified GTFS stops file saved to {output_file}")
