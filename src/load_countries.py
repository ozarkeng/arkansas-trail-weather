import os
from dotenv import load_dotenv
import geopandas as gpd
from sqlalchemy import create_engine, text
from pathlib import Path # <--- 1. IMPORT PATHLIB

# Load environment variables from .env file
load_dotenv()

# --- 1. GET DATABASE CREDENTIALS ---
DB_USER = os.getenv("POSTGRES_ROOT_USER")
DB_PASSWORD = os.getenv("POSTGRES_ROOT_PASSWORD")
DB_NAME = os.getenv("POSTGRES_ROOT_DB")
DB_HOST = "postgres-gis"
DB_PORT = "5432"

# --- 2. CREATE DATABASE CONNECTION ---
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

print("Database connection established.")

# --- 3. READ THE SHAPEFILE ---
# Construct an absolute path to the data file relative to THIS script's location
# This makes the script independent of the working directory it's called from.
SCRIPT_DIR = Path(__file__).parent
shapefile_path = SCRIPT_DIR / "data" / "ne_110m_admin_0_countries.shp" # <--- 2. UPDATE THIS

print(f"Reading shapefile from: {shapefile_path}")
gdf = gpd.read_file(shapefile_path)

# --- 4. LOAD DATA INTO POSTGIS ---
# The table will be named 'countries'
schema_name = "test"
table_name = "countries"

print(f"Loading data into table '{schema_name}.{table_name}'...")

# Check if the schema exists, if not create it 
with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
    conn.commit()

# Use GeoPandas to_postgis() method
gdf.to_postgis(
    name=table_name,
    schema="test",
    con=engine,
    if_exists="replace"
)

print("Data loaded successfully!")
