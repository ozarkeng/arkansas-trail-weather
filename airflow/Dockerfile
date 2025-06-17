# Start from the official Airflow image
FROM apache/airflow:2.9.2

# Switch to the root user to install system-level packages
USER root

# Install OS-level build tools and the GDAL development library.
# This gives the 'airflow' user the compilers (like gcc) and headers it needs.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libgdal-dev \
&& rm -rf /var/lib/apt/lists/*

# Now, switch back to the airflow user for the rest of the build and for runtime
USER airflow

# Copy your project's requirements file
COPY requirements.txt .

# Now, running pip as the 'airflow' user will succeed because the underlying
# build tools (gcc, etc.) are installed and available in the system.
RUN pip install --no-cache-dir -r requirements.txt
