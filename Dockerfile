# Start from the official Airflow image you are using
FROM apache/airflow:3.0.2

# Switch to the root user to install system-level dependencies for GeoPandas
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    g++ && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the non-root 'airflow' user for better security
USER airflow

# Copy the requirements file from your project root into the image
COPY requirements.txt /

# Install the Python packages listed in the requirements file
RUN pip install --no-cache-dir -r /requirements.txt
