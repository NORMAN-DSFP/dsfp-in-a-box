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
    libbz2-dev \
    librsvg2-dev \
    libuv1-dev \
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

# Install and verify the chemistry runtime dependency.
RUN R -e "install.packages('BiocManager', repos='https://cloud.r-project.org')" && \
    R -e "BiocManager::install('ChemmineR', ask=FALSE, update=FALSE)"
RUN R -e "options(timeout=600); urls <- c('https://bioconductor.statistik.tu-dortmund.de/packages/3.18/bioc/src/contrib/ChemmineR_3.54.0.tar.gz', 'https://ftp.gwdg.de/pub/misc/bioconductor/packages/3.18/bioc/src/contrib/ChemmineR_3.54.0.tar.gz'); archive <- tempfile(fileext='.tar.gz'); for (url in urls) { if (requireNamespace('ChemmineR', quietly=TRUE)) break; if (!inherits(try(download.file(url, archive, mode='wb')), 'try-error')) try(install.packages(archive, repos=NULL, type='source')) }; unlink(archive)"
RUN R -e "stopifnot(requireNamespace('ChemmineR', quietly=TRUE))"

# Expose the port Plumber will run on
EXPOSE 8001

# Command to start Plumber
CMD ["R", "-e", "pr <- plumber::plumb('semiquantification.R'); pr$run(host='0.0.0.0', port=8001)"]
