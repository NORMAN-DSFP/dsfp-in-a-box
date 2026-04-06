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

# Copy your Plumber API file into the container  
COPY semiquantification.R /app/semiquantification.R

# Install plumber first and test it
RUN R -e "install.packages('plumber', repos='https://cloud.r-project.org')" && \
    R -e "library('plumber'); cat('SUCCESS: Plumber installed and loaded!\n')"

# Install other required packages
RUN R -e "install.packages(c('rjson', 'readr'), repos='https://cloud.r-project.org')"

# Install BiocManager (skip chemistry packages for now to test plumber)
RUN R -e "install.packages('BiocManager', repos='https://cloud.r-project.org')" && \
    R -e "BiocManager::install('ChemmineR')"

# Expose the port Plumber will run on
EXPOSE 8001

# Command to start Plumber
CMD ["R", "-e", "pr <- plumber::plumb('semiquantification.R'); pr$run(host='0.0.0.0', port=8001)"]
