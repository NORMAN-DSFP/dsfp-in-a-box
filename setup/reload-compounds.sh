#!/bin/sh

echo "Starting compounds data reload..."
echo "Waiting for Elasticsearch..."
until curl -s http://elasticsearch:9200 > /dev/null; do
    sleep 5
done
echo "Elasticsearch is ready!"

# Function to download and process compounds
download_and_process_compounds() {
    local compounds_url="https://dsfp.norman-data.eu/sites/default/files/uploaded_resources/compounds.zip"
    local temp_dir="/tmp/compounds_reload"
    
    echo "Creating temporary directory..."
    mkdir -p "$temp_dir"
    cd "$temp_dir"
    
    echo "Downloading compounds.zip from $compounds_url..."
    if curl -L -o compounds.zip "$compounds_url"; then
        echo "Downloaded compounds.zip successfully"
    else
        echo "Failed to download compounds.zip"
        return 1
    fi
    
    echo "Extracting compounds.zip..."
    if unzip -q compounds.zip; then
        echo "Extracted compounds.zip successfully"
    else
        echo "Failed to extract compounds.zip"
        rm -f compounds.zip
        return 1
    fi
    
    echo "Looking for compounds.json file..."
    compounds_file=$(find . -name "compounds.json" -type f | head -1)
    
    if [ -z "$compounds_file" ]; then
        echo "compounds.json not found in the extracted files"
        ls -la
        rm -rf "$temp_dir"
        return 1
    fi
    
    echo "Found compounds.json at: $compounds_file"
    
    echo "Converting compounds data to bulk format..."
    if python3 /convert_bulk.py < "$compounds_file" > compounds-bulk.json; then
        echo "Converted to bulk format successfully"
    else
        echo "Failed to convert to bulk format"
        rm -rf "$temp_dir"
        return 1
    fi
    
    echo "Deleting existing compounds index..."
    curl -X DELETE "http://elasticsearch:9200/dsfp-compounds-1"
    sleep 2
    
    echo "Recreating compounds index..."
    curl -X PUT "http://elasticsearch:9200/dsfp-compounds-1" -H "Content-Type: application/json" -d @/compounds-mappings.json
    sleep 2
    
    echo "Loading compounds data in chunks..."
    
    # Split into smaller chunks (2000 lines = 1000 documents)
    split -l 2000 compounds-bulk.json chunk_
    
    total_uploaded=0
    
    for chunk in chunk_*; do
        echo "Uploading $chunk..."
        
        response=$(curl -s -w "%{http_code}" -X POST "http://elasticsearch:9200/dsfp-compounds-1/_bulk" -H "Content-Type: application/x-ndjson" --data-binary @"$chunk")
        
        http_code=$(echo "$response" | tail -c 4)
        
        if [ "$http_code" = "200" ]; then
            chunk_count=$(($(wc -l < "$chunk") / 2))
            total_uploaded=$((total_uploaded + chunk_count))
            echo "$chunk uploaded ($chunk_count docs)"
        else
            echo "$chunk failed (HTTP $http_code)"
            echo "$response" | head -c -4 | tail -c 200
        fi
        
        sleep 1
    done
    
    echo "Cleaning up temporary files..."
    rm -rf "$temp_dir"
    
    echo "Total uploaded: $total_uploaded documents"
    return 0
}

# Always download and reload compounds
if download_and_process_compounds; then
    echo "Compounds reloaded successfully"
else
    echo "Failed to reload compounds"
    exit 1
fi

final_count=$(curl -s "http://elasticsearch:9200/dsfp-compounds-1/_count" | grep -o '"count":[0-9]*' | cut -d':' -f2)
echo "Final document count: $final_count"
echo "Compounds reload completed"
