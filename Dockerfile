# Start from the official Airflow image that matches my docker-compose.yaml
FROM apache/airflow:3.0.2

# Switch to the root user to install system-level packages
USER root

# Install OS-level build tools and the GDAL development library needed for geospatial packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libgdal-dev \
&& rm -rf /var/lib/apt/lists/*

# Switch back to the non-root airflow user for the rest of the build and for runtime security
USER airflow

# Copy your project's requirements file into the image
COPY requirements.txt .

# Install your Python packages as the 'airflow' user.
# This will succeed because the underlying build tools are now available.
RUN pip install --no-cache-dir -r requirements.txt
