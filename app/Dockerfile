# Start from a slim Python base image
FROM python:3.11-slim

# Set the working directory inside the container to /app
WORKDIR /app

# Copy the requirements file into the container at /app
COPY app-requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r app-requirements.txt

# Copy the rest of the application's source code (e.g., home.py) into the container
COPY . .

# Expose the default port that Streamlit runs on
EXPOSE 8501

# The command to run when the container starts
CMD ["streamlit", "run", "home.py"]
