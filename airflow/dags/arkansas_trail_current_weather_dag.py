import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

# --- 1. PARAMS ---
URL = "https://api.openweathermap.org/data/2.5/weather"
API_KEY = os.environ.get("OPENWEATHER_API_KEY")
UPDATED_DATE = pendulum.now("America/Chicago")
CONFIG_FILEPATH = Path("/opt/airflow/dags/data/arkansas_trails.csv")
# CONFIG_FILEPATH = Path("airflow/dags/data/arkansas_trails.csv")
POSTGRES_CONN_ID = "postgis_data_db"
SCHEMA_NAME = "arkansas_trails"
TABLE_NAME = "current_weather"
DEFAULT_PARAMS = {
    "appid": API_KEY,
    "units": "imperial",
}
FIELDS_DICT = {
    "weather_description": "weather.0.description",
    "temperature": "main.temp",
    "feels_like": "main.feels_like",
    "temperature_min": "main.temp_min",
    "temperature_max": "main.temp_max",
    "pressure": "main.pressure",
    "humidity": "main.humidity",
    "wind_speed": "wind.speed",
    "wind_direction": "wind.deg",
    "wind_gust": "wind.gust",
    "updated_date": "dt",
    "sunrise_time": "sys.sunrise",
    "sunset_time": "sys.sunset",
}


# --- 2. UTILITY FUNCTIONS ---
def read_config() -> pd.DataFrame:
    """Returns a DataFrame with the coordinates of the Arkansas Trailhead."""

    if not CONFIG_FILEPATH.exists():
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_FILEPATH}")
    elif not CONFIG_FILEPATH.is_file():
        raise ValueError(f"Configuration path is not a file: {CONFIG_FILEPATH}")
    else:
        df = pd.read_csv(CONFIG_FILEPATH)
        if df.empty:
            raise ValueError("Configuration file is empty.")
        return df


# --- 2.a EXTRACT ---
def get_weather_data(lat: float, lon: float) -> dict:
    """Fetches weather data for the given latitude and longitude."""
    params = DEFAULT_PARAMS.copy()
    params["lat"] = lat
    params["lon"] = lon

    response = requests.get(URL, params=params)

    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch weather data: {response.status_code} - {response.text}"
        )

    return response.json()


def extract_weather_data(weather_data: dict) -> dict:
    """Extracts relevant fields from the weather data."""
    extracted_data = {}
    for field_name, path_string in FIELDS_DICT.items():
        keys = path_string.split(".")
        current_value = weather_data

        for i, key in enumerate(keys):
            if isinstance(current_value, dict):
                # If current_value is a dictionary, try to get the key
                current_value = current_value.get(key, None)
            elif isinstance(current_value, list):
                # If current_value is a list, we expect a numeric index for the next key
                try:
                    index = int(key)  # Try to convert key to an integer index
                    if 0 <= index < len(current_value):
                        current_value = current_value[index]
                    else:
                        current_value = None  # Index out of bounds
                except ValueError:
                    # The key was not a valid integer index for a list
                    current_value = None
            else:
                # If current_value is neither a dict nor a list, we can't go deeper
                current_value = None

            if current_value is None:
                break  # Stop if we couldn't find the current key/index

        extracted_data[field_name] = current_value

    return extracted_data


# --- 2.b TRANSFORM ---
def decode_timestamp(timestamp: int) -> pendulum.datetime:
    """Converts a Unix timestamp to python datetime object."""

    # dt_object = datetime.datetime.fromtimestamp(timestamp)
    dt_object = pendulum.from_timestamp(timestamp, tz="America/Chicago")
    return dt_object


def format_timestamp(timestamp: int) -> str:
    """Converts a Unix timestamp to a formatted time string."""

    # dt_object = datetime.datetime.fromtimestamp(timestamp)
    dt_object = pendulum.from_timestamp(timestamp, tz="America/Chicago")
    formatted_time = dt_object.strftime("%I:%M:%S %p")
    return formatted_time


def process_weather_data(extracted_data: dict):
    """Processes the extracted weather data by decoding timestamps and formatting them."""
    # Decode timestamps
    # both format_timestamp and decode_timestamp are used to process the timestamps
    # both take in Unix timestamps
    # process format_timestamp FIRST because we're overwriting the original timestamp with decode_timestamp

    updated_date = extracted_data.get("updated_date")
    sunrise_time = extracted_data.get("sunrise_time")
    sunset_time = extracted_data.get("sunset_time")

    extracted_data["updated_date_tz"] = format_timestamp(updated_date)
    extracted_data["sunrise_time_tz"] = format_timestamp(sunrise_time)
    extracted_data["sunset_time_tz"] = format_timestamp(sunset_time)
    extracted_data["updated_date"] = decode_timestamp(updated_date)
    extracted_data["sunrise_time"] = decode_timestamp(sunrise_time)
    extracted_data["sunset_time"] = decode_timestamp(sunset_time)

    return extracted_data


def dataframe_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Converts a DataFrame with 'lon' and 'lat' columns to a GeoDataFrame."""
    if "lon" not in df.columns or "lat" not in df.columns:
        raise ValueError("DataFrame must contain 'lon' and 'lat' columns.")

    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs="EPSG:4326"
    )
    return gdf


# ==========================
# --- DAG ---
# ==========================
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 6, 19, tz="America/Chicago"),
    schedule="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    tags=["geospatial", "weather"],
    doc_md="""
    ### Arkansas Trail Current Weather dag
    This DAG fetches current weather data for Arkansas trailheads and stores it in a PostGIS database.
    """,
)
def arkansas_trail_current_weather_dag():

    @task
    def fetch_and_process_weather_data():
        # Read the configuration file
        df = read_config()

        # Initialize a list to hold weather data
        weather_data_list = []

        # Iterate through each row in the DataFrame
        for _, row in df.iterrows():
            lat = row["lat"]
            lon = row["lon"]

            # Fetch weather data for the given coordinates
            try:
                weather_data = get_weather_data(lat, lon)
                extracted_data = extract_weather_data(weather_data)
                processed_data = process_weather_data(extracted_data)
                config_data = row.to_dict()
                dataframe_dict = config_data | processed_data
                weather_data_list.append(dataframe_dict)
            except Exception as e:
                print(f"Error fetching weather data for {lat}, {lon}: {e}")

        # Convert the list of weather data to a DataFrame
        weather_df = pd.DataFrame(weather_data_list)
        weather_gdf = dataframe_to_geodataframe(weather_df)

        return weather_gdf

    @task
    def load_weather_to_postgis(gdf: gpd.GeoDataFrame, conn_id: str = POSTGRES_CONN_ID):
        """Loads a GeoDataFrame into a PostGIS table."""
        # Create a PostgresHook to connect to the database
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        engine = pg_hook.get_sqlalchemy_engine()

        # Ensure the schema exists
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))

        # Load the GeoDataFrame into PostGIS
        gdf.to_postgis(
            name=TABLE_NAME,
            con=engine,
            schema=SCHEMA_NAME,
            if_exists="replace",
            index=True,
        )
        print("Weather data loaded into PostGIS successfully.")

    # Define the DAG tasks
    weather_gdf = fetch_and_process_weather_data()
    load_weather_to_postgis(weather_gdf)


# --- 3. MAIN EXECUTION ---
# if __name__ == "__main__":
#    # Read the configuration file
#    df = read_config()
#
#    # Initialize a list to hold weather data
#    weather_data_list = []
#
#    # Iterate through each row in the DataFrame
#    for index, row in df.iterrows():
#        lat = row["lat"]
#        lon = row["lon"]
#        location = row["location"]
#
#        # Fetch weather data for the given coordinates
#        try:
#            weather_data = get_weather_data(lat, lon)
#            extracted_data = extract_weather_data(weather_data)
#            processed_data = process_weather_data(extracted_data)
#            config_data = row.to_dict()
#            dataframe_dict = config_data | extracted_data
#            weather_data_list.append(dataframe_dict)
#        except Exception as e:
#            print(f"Error fetching weather data for {lat}, {lon}: {e}")
#
#    # Convert the list of weather data to a DataFrame
#    weather_df = pd.DataFrame(weather_data_list)
#    weather_gdf = dataframe_to_geodataframe(weather_df)
#
#    # Print or save the weather DataFrame as needed
#    print(weather_gdf.info())
#    print()
#    print(weather_gdf.head())


###### EXAMPLE RESPONSE from "https://api.openweathermap.org/data/2.5/weather?lat=35.8910241&lon=-93.4405218&appid=77bc72e6e29bff758ae427a436603c86&units=imperial"
# {
#    "coord": {"lon": -93.4405, "lat": 35.891},
#    "weather": [
#        {"id": 804, "main": "Clouds", "description": "overcast clouds", "icon": "04n"}
#    ],
#    "base": "stations",
#    "main": {
#        "temp": 73.69,
#        "feels_like": 75.04,
#        "temp_min": 71.46,
#        "temp_max": 74.79,
#        "pressure": 1012,
#        "humidity": 91,
#        "sea_level": 1012,
#        "grnd_level": 945,
#    },
#    "visibility": 10000,
#    "wind": {"speed": 7.45, "deg": 187, "gust": 26.13},
#    "clouds": {"all": 98},
#    "dt": 1750220572,
#    "sys": {
#        "type": 2,
#        "id": 2099403,
#        "country": "US",
#        "sunrise": 1750157825,
#        "sunset": 1750210347,
#    },
#    "timezone": -18000,
#    "id": 4116400,
#    "name": "Jasper",
#    "cod": 200,
# }
