#!/bin/sh

echo "Waiting for Elasticsearch..."

until curl -s "$ELASTIC_URL" >/dev/null 2>&1; do
  sleep 2
done

echo "Elasticsearch is ready!"

# Check if the screening index already exists
INDEX_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" "$ELASTIC_URL/dsfp-screening-index")

if [ "$INDEX_EXISTS" = "200" ]; then
  echo "Index 'dsfp-screening-index' already exists - skipping creation"
else
  # Create the screening index
  echo "Creating index 'dsfp-screening-index'..."
  curl -X PUT "$ELASTIC_URL/dsfp-screening-index" \
    -H "Content-Type: application/json" \
    -d @mappings.json
  echo "Index 'dsfp-screening-index' created successfully."
fi

# Check if the compounds index already exists
COMPOUNDS_INDEX_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" "$ELASTIC_URL/dsfp-compounds-1")

if [ "$COMPOUNDS_INDEX_EXISTS" = "200" ]; then
    echo "Index 'dsfp-compounds-1' already exists - skipping creation"
    
    # Check if index has any documents
    DOC_COUNT=$(curl -s "http://elasticsearch:9200/dsfp-compounds-1/_count" | grep -o '"count":[0-9]*' | cut -d':' -f2)
    echo "Current document count in dsfp-compounds-1: $DOC_COUNT"
    
    # Only load data if index is empty
    if [ "$DOC_COUNT" = "0" ]; then
        echo "Loading compounds data into dsfp-compounds-1 index in chunks..."
        
        # Split the file into smaller chunks (10000 lines each)
        split -l 10000 compounds.json chunk_
        
        TOTAL_UPLOADED=0
        for chunk in chunk_*; do
            echo "Uploading $chunk..."
            RESPONSE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://elasticsearch:9200/_bulk" -H "Content-Type: application/x-ndjson" --data-binary "@$chunk")
            
            if [ "$RESPONSE_CODE" = "200" ]; then
                CHUNK_LINES=$(wc -l < "$chunk")
                TOTAL_UPLOADED=$((TOTAL_UPLOADED + CHUNK_LINES))
                echo "✓ $chunk uploaded successfully ($CHUNK_LINES documents)"
            else
                echo "✗ $chunk upload failed (HTTP $RESPONSE_CODE)"
            fi
            
            # Clean up chunk file
            rm -f "$chunk"
        done
        
        echo "Total documents uploaded: $TOTAL_UPLOADED"
    else
        echo "Index already contains $DOC_COUNT documents - skipping data loading"
    fi
else
    # Create the compounds index
    echo "Creating index 'dsfp-compounds-1'..."
    curl -X PUT "$ELASTIC_URL/dsfp-compounds-1" \
        -H "Content-Type: application/json" \
        -d @compounds-mappings.json
    echo "Index 'dsfp-compounds-1' created successfully."
    
    # Load compounds data in chunks
    echo "Loading compounds data into dsfp-compounds-1 index in chunks..."
    
    # Split the file into smaller chunks (10000 lines each)
    split -l 10000 compounds.json chunk_
    
    TOTAL_UPLOADED=0
    for chunk in chunk_*; do
        echo "Uploading $chunk..."
        RESPONSE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://elasticsearch:9200/_bulk" -H "Content-Type: application/x-ndjson" --data-binary "@$chunk")
        
        if [ "$RESPONSE_CODE" = "200" ]; then
            CHUNK_LINES=$(wc -l < "$chunk")
            TOTAL_UPLOADED=$((TOTAL_UPLOADED + CHUNK_LINES))
            echo "✓ $chunk uploaded successfully ($CHUNK_LINES documents)"
        else
            echo "✗ $chunk upload failed (HTTP $RESPONSE_CODE)"
        fi
        
        # Clean up chunk file
        rm -f "$chunk"
    done
    
    echo "Total documents uploaded: $TOTAL_UPLOADED"
fi

# Final document count check
FINAL_COUNT=$(curl -s "http://elasticsearch:9200/dsfp-compounds-1/_count" | grep -o '"count":[0-9]*' | cut -d':' -f2)
echo "Final document count in dsfp-compounds-1: $FINAL_COUNT"

echo "Initialization completed"