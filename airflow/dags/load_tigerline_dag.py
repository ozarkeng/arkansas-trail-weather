import os
from pathlib import Path

import geopandas as gpd
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from sqlalchemy import text

# --- 1. PARAMETERS ---
POSTGRES_CONN_ID = "postgis_data_db"
DATA_DIR = Path("/opt/airflow/dags/data")

# --- 2. DEFINE DATASETS ---
DATASETS = {
    "us_state": {
        "url": "https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip",
        "schema": "tigerline",
        "table": "us_state",
    },
    "us_county": {
        "url": "https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip",
        "schema": "tigerline",
        "table": "us_county",
    },
}


# --- 3. DEFINE DAG ---
@dag(
    dag_id="geospatial_data_ingestion", 
    start_date=datetime(2025, 6, 17),
    schedule=None,
    catchup=False,
    tags=["geospatial", "postgres", "etl"],
    doc_md="""
    ### Geospatial Data Ingestion DAG 
    This DAG downloads TIGER shapefiles and loads them directly from the zip
    archive into a PostGIS database.
    """,
)
def geospatial_data_ingestion_dag():

    # --- 3. DEFINE TASKS ---
    @task
    def download_zip_file(url: str, download_dir: Path) -> str:
        """Downloads a file from a URL and saves it locally.
        NOTE: Airflow does not support Path objects directly in tasks -> convert to str.
        """
        download_dir.mkdir(parents=True, exist_ok=True)
        zip_filename = url.split("/")[-1]
        zip_filepath = download_dir / zip_filename

        with requests.get(url, stream=True, verify=False) as r:
            r.raise_for_status()
            with open(zip_filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return str(zip_filepath)

    @task
    def load_zip_to_postgis(
        zip_filepath: str, schema_name: str, table_name: str, conn_id: str
    ):
        """Loads a shapefile from a ZIP archive directly into a PostGIS table."""
        # GeoPandas reads the shapefile from within the zip file.
        gdf = gpd.read_file(zip_filepath)

        # Create a PostgresHook to connect to the database
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        engine = pg_hook.get_sqlalchemy_engine()

        # Ensure the schema exists
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))

        # Load the GeoDataFrame into PostGIS
        gdf.to_postgis(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists="replace",
            index=True,
        )
        print(f"Successfully loaded {len(gdf)} records into {schema_name}.{table_name}")

    @task
    def cleanup_zip_file(zip_filepath: str):
        """Deletes the downloaded zip file."""
        os.remove(zip_filepath)
        print(f"Cleaned up {zip_filepath}.")

    # --- 5. CREATE DYNAMIC TASKS AND SET DEPENDENCIES ---
    for name, config in DATASETS.items():
        # Download the zip file
        zip_file_path = download_zip_file(url=config["url"], download_dir=DATA_DIR)

        # Pass the zip file path directly to the loading task
        load_task = load_zip_to_postgis(
            zip_filepath=zip_file_path,
            schema_name=config["schema"],
            table_name=config["table"],
            conn_id=POSTGRES_CONN_ID,
        )

        # Cleanup task to remove the zip file after loading
        cleanup_task = cleanup_zip_file(zip_filepath=zip_file_path)

        # Set dependencies for the pipeline
        # NOTE: the download task is not explicitly defined as a separate task, but is implied
        load_task >> cleanup_task


# --- 6. INSTANTIATE THE DAG ---
geospatial_data_ingestion_dag()

