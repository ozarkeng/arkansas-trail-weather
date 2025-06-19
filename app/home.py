import os

import folium
import geopandas as gpd
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine
from streamlit_folium import st_folium

load_dotenv()

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Arkansas Trail Weather Dashboard",
    page_icon="🗺️",
    layout="centered"
)

# --- DATABASE CONNECTION ---
# I will get the connection details from environment variables
# when I run this in Docker Compose later.
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

# Create the connection string
connection_string = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


@st.cache_resource
def get_db_engine():
    """Creates and caches a database engine connection."""
    return create_engine(connection_string)


@st.cache_data(ttl=60)  # Cache the data for 1 minute
def load_data(_engine):
    """Loads the weather data from the PostGIS database."""
    try:
        # SQL query to fetch the latest weather reading for each location
        sql_query = """
            SELECT
                *,
                updated_date AT TIME ZONE 'America/Chicago' AS updated_date_cst
            FROM arkansas_trails.current_weather
        """
        gdf = gpd.read_postgis(sql_query, _engine, geom_col="geometry")
        return gdf
    except Exception as e:
        st.error(f"Error loading data from the database: {e}")
        return gpd.GeoDataFrame()  # Return empty GeoDataFrame on error


# --- PAGE LAYOUT ---
st.markdown("<h1 style='text-align: center; color: CadetBlue; font-weight: 1000;'>Arkansas Trails - Current Weather</h1>", unsafe_allow_html=True)

engine = get_db_engine()
weather_data = load_data(engine)


if not weather_data.empty:

    # --- 3a. Initialize Session State ---
    # I use session_state to remember which point was clicked.
    if "selected_point_index" not in st.session_state:
        st.session_state.selected_point_index = 0

    # --- 3b. Create the Interactive Map ---
    # Center the map on the average coordinates of the trailheads
    map_center = [weather_data['lat'].mean(), weather_data['lon'].mean()]
    m = folium.Map(location=map_center, tiles="USGS_USImageryTopo", zoom_start=7)

    # Add a marker for each trailhead
    for index, row in weather_data.iterrows():
        folium.Marker(
            location=[row['lat'], row['lon']],
            # The tooltip appears on hover
            tooltip=row['location'],
            # The popup is used to identify which marker was clicked
            popup=row['location']
        ).add_to(m)

    # --- 3c. Render the Map and Capture Clicks ---
    map_data = st_folium(m, height=400, width=1000)

    # Check if a marker was clicked
    if map_data and map_data.get("last_object_clicked_popup"):
        clicked_location = map_data["last_object_clicked_popup"]
        # Find the index of the clicked location
        clicked_index = weather_data[weather_data['location'] == clicked_location].index[0]
        st.session_state.selected_point_index = clicked_index

    # Get the data for the currently selected point
    selected_point_data = weather_data.iloc[st.session_state.selected_point_index]

    # --- 3d. Display Metrics for the Selected Point ---
    #st.write(selected_point_data)
    st.divider()
    st.markdown(f"<h2 style='text-align: center; color: CadetBlue;'>Weather for {selected_point_data['location']} - {selected_point_data['weather_description'].title()}</h2>", unsafe_allow_html=True)

    # Update logic:
    # If updated_date is > 10 minutes from now() show in red background and dark red text 
    # If updated_date <= 10 minutes from now() show in green background and dark green text
    now = pd.Timestamp.now(tz='America/Chicago')
    
    # First, ensure the value is a pandas Timestamp object
    updated_time_naive = pd.to_datetime(selected_point_data['updated_date_cst'])
    updated_time_aware = updated_time_naive.tz_localize('America/Chicago')
    
    # Compare the two "aware" timestamps
    if updated_time_aware > now - pd.Timedelta(minutes=10):
        background_color = "forestgreen"
        text_color = "ghostwhite"
        message = f"Last Updated: {selected_point_data['updated_date_cst'].strftime('%-I:%M %p %Z')} - Data is up-to-date - Pipeline HEALTHY."
    else:
        background_color = "lightcoral"
        text_color = "darkred"
        message = f"Last Updated: {selected_point_data['updated_date_cst'].strftime('%-I:%M %p %Z')} - Data is outdated - Pipeline HEALTH CHECK REQUIRED!"

    # Display the message with the appropriate colors
    st.markdown(
    f"""
    <div style='display: flex; justify-content: center; margin-bottom: 20px;'>  <div style='
            display: inline-block; 
            padding: 5px 10px; 
            border-radius: 15px; 
            background-color: {background_color}; 
            color: {text_color}; 
            font-weight: bold;
        '>
            {message}
        </div>
    </div>
    """,
    unsafe_allow_html=True
    )
    
    # Metrics
    col1, col2, col3 = st.columns(3, vertical_alignment="center")
    with col1:
        st.metric(label="Temperature", value=f"{selected_point_data['temperature']:.1f} °F", border=True)
        st.metric(label="Feels Like", value=f"{selected_point_data['feels_like']:.1f} °F", border=True)
    with col2:
        st.metric(label="Humidity", value=f"{selected_point_data['humidity']}%", border=True)
        st.metric(label="Wind Speed", value=f"{selected_point_data['wind_speed']:.1f} mph", border=True)
    with col3:
        st.metric(label="Sunrise", value=selected_point_data['sunrise_time'].strftime('%-I:%M %p'), border=True)
        st.metric(label="Sunset", value=selected_point_data['sunset_time'].strftime('%-I:%M %p'), border=True)

    st.divider()

else:
    st.warning("Could not load weather data. The data pipeline may not have run yet.")

