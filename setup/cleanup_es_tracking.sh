#!/bin/bash
# Clean up Elasticsearch tracking index (migration to DuckDB)

echo "DuckDB Migration: Cleaning up Elasticsearch tracking data..."

# Check if tracking index exists
if curl -s -f "http://elasticsearch:9200/dsfp-tracking-index" > /dev/null; then
    echo "Found existing tracking index - backing up before deletion..."
    
    # Create backup
    echo "Creating backup of tracking data..."
    curl -s "http://elasticsearch:9200/dsfp-tracking-index/_search?size=10000" > /data/elasticsearch_tracking_backup.json
    
    # Delete the tracking index
    echo "Deleting tracking index..."
    curl -X DELETE "http://elasticsearch:9200/dsfp-tracking-index"
    
    echo "✓ Tracking index cleaned up - data backed up to /data/elasticsearch_tracking_backup.json"
else
    echo "No tracking index found - already clean"
fi

echo "Migration cleanup completed - tracking now uses DuckDB"
