FROM ubuntu:20.04

# Install Python and required dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy GenForm executable and make it executable
COPY GenForm /opt/genform/GenForm
RUN chmod +x /opt/genform/GenForm

# Copy the Python wrapper script
COPY run_genform.py /app/run_genform.py
RUN chmod +x /app/run_genform.py

# Create temp directory for GenForm files
RUN mkdir -p /tmp/genform

# Keep the container running instead of exiting immediately
CMD ["tail", "-f", "/dev/null"]