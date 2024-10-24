import requests
import pandas as pd
import folium
import geopandas as gpd
from shapely.geometry import Point
import plotly.express as px
import sqlite3
import logging

# URL of the Chicago Traffic Tracker dataset
url = "https://data.cityofchicago.org/resource/sxs8-h27x.json"

# Configure logging
logging.basicConfig(level=logging.INFO)


# Fetch the contents of the URL
response = requests.get(url)
if response.status_code != 200:
    print("Failed to retrieve data.")
    exit()

# Convert the JSON data into a DataFrame
df = pd.DataFrame(response.json())

# Check for missing values in latitude/longitude columns
df = df.dropna(subset=["start_latitude", "start_longitude"])

# Create a GeoDataFrame
geometry = [Point(xy) for xy in zip(df['start_longitude'], df['start_latitude'])]
gdf = gpd.GeoDataFrame(df, geometry=geometry)

# Set the coordinate reference system (CRS)
gdf.set_crs(epsg=4326, inplace=True)

# Create a buffer around each intersection (e.g., 100 meters)
buffer_distance = 0.001  # Approx. 100 meters in degrees
gdf['buffer'] = gdf.geometry.buffer(buffer_distance)

# Find adjacent intersections
adjacent_intersections = []
chosen_intersections = []
adjacent_rows = []

index = 0
for idx, row in gdf.iterrows():
    adjacent = gdf[gdf['buffer'].intersects(row['geometry']) & (gdf.index != idx)]
    if not adjacent.empty:
        adjacent_intersections.append({
            'intersection': row['street'],
            'adjacent': adjacent['street'].tolist()
        })
        adjacent_rows.append(row)
        print(row['street'])
        chosen_intersections.append(row['street'])
        index+=1
    
    if index == 7:
        break


def store_to_sqlite(df, db_name, table_name):
    #Store data in sql database
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info(f"Data successfully stored in {db_name} under {table_name}.")
    except sqlite3.Error as e:
        logging.error(f"Database error: {str(e)}")
    finally:
        conn.close()

def clean_data(df):
    """Clean the DataFrame by handling missing values."""
    # Check for missing values
    logging.info("Checking for missing values...")
    missing_values = df.isnull().sum()
    logging.info(f"Missing values: {missing_values[missing_values > 0]}")

    # Forward fill missing values
    df.fillna(method='ffill', inplace=True)

    # Convert any dictionary-type columns to strings
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, dict) else x)

    return df

def filter_negative_values(df):
    """Filter out rows with negative values in specified columns."""
    # Specify the columns to check for negative values
    columns_to_check = df.select_dtypes(include=['float64','int']).columns

    # Create a mask for rows without negative values in the specified columns
    mask = (df[columns_to_check] >= 0).all(axis=1)

    # Filter the DataFrame
    filtered_df = df[(df[columns_to_check] >= 0).all(axis=1)]

    return filtered_df

# Convert results to DataFrame, clean data, and store in the database
adjacent_df = clean_data(pd.DataFrame(adjacent_rows))
# Convert any dictionary-type columns to strings
for col in adjacent_df.columns:
        if adjacent_df[col].dtype == 'object':
            adjacent_df[col] = adjacent_df[col].apply(lambda x: str(x) if isinstance(x, dict) else x)
store_to_sqlite(filter_negative_values(adjacent_df),'traffic_data.db','traffic_table')

# Create a Folium map centered around Chicago
chicago_map = folium.Map(location=[41.8781, -87.6298], zoom_start=12)

# Add markers for traffic congestion records and adjacent intersections
for index, row in df.iterrows():
    start_lat = row.get("start_latitude")
    start_lon = row.get("start_longitude")
    end_lat = row.get("end_latitude")
    end_lon = row.get("end_longitude")


for item in adjacent_intersections:
    for adjacent in item['adjacent']:
        # Add a marker for each adjacent intersection
        folium.Marker(
            location=[gdf.loc[gdf['street'] == adjacent, 'start_latitude'].values[0],
                      gdf.loc[gdf['street'] == adjacent, 'start_longitude'].values[0]],
            popup=f"Adjacent Intersection: {"start - " + adjacent}",
            icon=folium.Icon(color="green", icon="play"),
        ).add_to(chicago_map)

        folium.Marker(
            location=[gdf.loc[gdf['street'] == adjacent, 'end_latitude'].values[0],
                      gdf.loc[gdf['street'] == adjacent, 'end_longitude'].values[0]],
            popup=f"Adjacent Intersection: {"end - " + adjacent}",
            icon=folium.Icon(color="red", icon="pause"),
        ).add_to(chicago_map)

# Save the map with markers
chicago_map.save('chicago_folium_map.html')

df['start_latitude'] = pd.to_numeric(df['start_latitude'])
df['start_longitude'] = pd.to_numeric(df['start_longitude'])

# Optional: Density maps by hour
if 'time' in df.columns:
    df['hour'] = pd.to_datetime(df['time']).dt.hour  # Ensure the time is parsed correctly
    index = 1
    for hour in df['hour'].unique():
        each_hour = df[df['hour'] == hour]
        if not each_hour.empty:  # Only create a map if there's data for that hour
            fig = px.density_mapbox(
                data_frame=each_hour,
                lat="start_latitude",
                lon="start_longitude",
                mapbox_style='open-street-map',
                radius=5,
                zoom=9,
            )
            fig.update_layout(coloraxis_showscale=True, hovermode='closest')

            fig.write_html(f"chicago_traffic_heat_{index}_map.html")
            index += 1
else:
    print("The 'time' column is not present in the dataset.")
