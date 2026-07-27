# jsoncreate - final step of the sample processing pipeline (see runtime.R).
#
# One-off batch container, invoked the same way as models/parse (see comment
# there). All dependencies are on CRAN.
FROM r-base:4.3.1

USER root

RUN apt-get update && apt-get install -y \
    curl \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libgit2-dev \
    libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY runtime.R /app/runtime.R

RUN R -e "install.packages(c('readr', 'rjson', 'aws.s3'), repos='https://cloud.r-project.org')"
