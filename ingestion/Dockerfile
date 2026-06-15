# =============================================================================
#  Dockerfile for the ingestion layer
#
#  WHAT THIS DOES:
#    Packages our Python ingestion scripts into a Docker container image.
#    Cloud Run Jobs run this container on GCP's infrastructure.
#
#  WHY CONTAINERS?
#    "It works on my machine" is not a deployment strategy.
#    A container bundles the Python version, libraries, and code together.
#    The exact same image runs locally and on GCP.
#
#  BUILD:
#    gcloud builds submit --tag gcr.io/citibike-pipeline-499418/citibike-ingestion:latest
#  RUN LOCALLY:
#    docker run -e GOOGLE_APPLICATION_CREDENTIALS=/key.json \
#               -v /path/to/key.json:/key.json \
#               citibike-ingestion python fetch_realtime_gbfs.py
# =============================================================================

# Use the official slim Python image (smaller = faster startup in Cloud Run)
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements first — Docker caches this layer if requirements don't change
# (speeds up rebuilds when only code changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion scripts
COPY fetch_trip_history.py .
COPY fetch_realtime_gbfs.py .

# Copy the config file (one level up in local, but flattened in container)
COPY ../config/config.yaml /app/config/config.yaml

# Default command (overridden by Cloud Run Job args)
CMD ["python", "fetch_realtime_gbfs.py"]
