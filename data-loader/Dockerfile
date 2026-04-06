FROM python:3.11-slim

WORKDIR /app

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY load_data.py .
COPY config.py .

# Create data directory
RUN mkdir -p /app/data

# Don't run automatically - will be called via docker compose run
CMD ["sleep", "infinity"]
