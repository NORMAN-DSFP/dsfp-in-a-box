FROM python:3.11-slim

# Install Node.js and npm
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages with pandas 2.2.0 which supports numpy >= 2.0
RUN pip install --no-cache-dir --force-reinstall \
    requests==2.31.0 \
    "numpy>=2.0.0" \
    pandas==2.2.3 \
    duckdb

# Create app directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install Node.js dependencies
RUN npm install
RUN npm install -g nodemon

# Copy source code
COPY . .

# Expose port
EXPOSE 9000

# Start the server
CMD ["node", "server.js"]
