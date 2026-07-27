# componentize - second step of the sample processing pipeline (see runtime.R).
#
# One-off batch container, invoked the same way as models/parse (see comment
# there). Memory-hungry: pattern.search is O(n^2), so run with at least 4GB
# available to the container.
FROM r-base:4.3.1

USER root

RUN apt-get update && apt-get install -y \
    curl \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libgit2-dev \
    libsodium-dev \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    libicu-dev \
    libuv1-dev \
    libnetcdf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY runtime.R /app/runtime.R

RUN R -e "install.packages(c('readr', 'aws.s3', 'enviPat', 'BiocManager', 'remotes'), repos='https://cloud.r-project.org')"

# xcms depends on ncdf4, mzR, and MSnbase. Install the Bioconductor chain
# explicitly so the missing piece is obvious if one of them regresses.
RUN R -e "BiocManager::install(c('ncdf4', 'mzR', 'MSnbase', 'xcms'), update = FALSE, ask = FALSE)"

# 'nontargetData' is a required (hard) dependency of nontarget and is not on
# CRAN. mgcv (also a hard dependency) already ships bundled with base R.
RUN R -e "remotes::install_github('blosloos/nontargetData')"

# nontarget's optional plothomolplotly() helper pulls in the tidyverse/plotly
# stack, which is unnecessary for this pipeline. Install a trimmed copy of the
# source with that helper and its imports removed so the core runtime functions
# can be installed without the heavy plotting dependency chain.
RUN set -eux; \
    curl -L -o /tmp/nontarget.tar.gz https://github.com/blosloos/nontarget/archive/refs/heads/master.tar.gz; \
    tar -xzf /tmp/nontarget.tar.gz -C /tmp; \
    rm -f /tmp/nontarget-master/R/plothomolplotly.R /tmp/nontarget-master/man/plothomolplotly.Rd; \
    sed -i '/plothomolplotly/d;/plotly/d;/tidyverse/d;/tibble/d;/tidyr/d;/ggplot2/d' /tmp/nontarget-master/NAMESPACE; \
    R -e "desc_path <- '/tmp/nontarget-master/DESCRIPTION'; desc <- read.dcf(desc_path); desc[, 'Imports'] <- 'enviPat (>= 2.0), nontargetData (>= 1.1), mgcv (>= 1.7-22)'; write.dcf(as.data.frame(desc), file = desc_path)"; \
    R CMD INSTALL /tmp/nontarget-master

