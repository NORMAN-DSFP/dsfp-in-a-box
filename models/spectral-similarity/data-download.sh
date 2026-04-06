#!/bin/sh
set -e

echo "Downloading ZIP file..."

curl -L "https://dsfp.norman-data.eu/sites/default/files/MassBank.zip" -o /tmp/MassBank.zip

echo "Unzipping..."

unzip /tmp/MassBank.zip -d /app

echo "ZIP downloaded and extracted to /app"
