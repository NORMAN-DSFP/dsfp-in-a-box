# Start from the official Plumber image
FROM r-base:4.3.1

USER root

# Install system libraries and tools
RUN apt-get update && apt-get install -y \
    unzip \
    curl \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libgit2-dev \
    libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy and run data download script
COPY data-download.sh /app/data-download.sh
RUN sed -i 's/\r$//' /app/data-download.sh && \
    chmod +x /app/data-download.sh && \
    sh /app/data-download.sh

# Copy your Plumber API file into the container
COPY spectral_similarity.R /app/spectral_similarity.R

# Install R packages
RUN R -e "install.packages('plumber', repos='https://cloud.r-project.org')"
RUN R -e "install.packages(c('jsonlite', 'readr', 'OrgMassSpecR'), repos='https://cloud.r-project.org')"

# Expose the port Plumber will run on
EXPOSE 8002

CMD ["R", "-e", "pr <- plumber::plumb('spectral_similarity.R'); pr$run(host='0.0.0.0', port=8002)"]
