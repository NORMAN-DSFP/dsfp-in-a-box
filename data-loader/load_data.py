#!/usr/bin/env python3
"""
DSFP Data Loader

Loads JSON files from a local data directory into Elasticsearch.
Processes all JSON/JSONL files found in the data directory and subdirectories.
"""

import json
import os
import sys
import time
import logging
from typing import List, Dict, Any, Generator
from pathlib import Path
import glob

import requests
from elasticsearch import Elasticsearch, helpers

import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self):
        self.es_client = None
        self.setup_elasticsearch()
    
    def setup_elasticsearch(self):
        """Initialize Elasticsearch client and wait for connection"""
        logger.info(f"Connecting to Elasticsearch at {config.ELASTICSEARCH_URL}")
        
        # Wait for Elasticsearch to be ready
        max_retries = 30
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{config.ELASTICSEARCH_URL}/_cluster/health")
                if response.status_code == 200:
                    break
            except requests.exceptions.RequestException:
                pass
            
            if attempt < max_retries - 1:
                logger.info(f"Waiting for Elasticsearch... (attempt {attempt + 1}/{max_retries})")
                time.sleep(2)
            else:
                logger.error("Failed to connect to Elasticsearch after maximum retries")
                sys.exit(1)
        
        self.es_client = Elasticsearch([config.ELASTICSEARCH_URL])
        logger.info("Elasticsearch client initialized successfully")
    
    def check_data_directory(self) -> bool:
        """Check if the data directory exists and contains JSON files"""
        if not os.path.exists(config.LOCAL_DATA_PATH):
            logger.error(f"Data directory not found: {config.LOCAL_DATA_PATH}")
            logger.info("Please upload and extract a ZIP file through the status dashboard")
            return False
        
        # Find JSON files in the directory
        json_files = self.find_json_files(config.LOCAL_DATA_PATH)
        if not json_files:
            logger.error(f"No JSON files found in {config.LOCAL_DATA_PATH}")
            return False
        
        logger.info(f"Found data directory: {config.LOCAL_DATA_PATH}")
        logger.info(f"Found {len(json_files)} JSON files to process")
        return True
    
    def find_json_files(self, directory: str) -> List[str]:
        """Recursively find all JSON files in a directory"""
        json_files = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.lower().endswith(('.json', '.jsonl')):
                    full_path = os.path.join(root, file)
                    # Check file size
                    file_size_mb = os.path.getsize(full_path) / (1024 * 1024)
                    if file_size_mb <= config.MAX_FILE_SIZE_MB:
                        json_files.append(full_path)
                    else:
                        logger.warning(f"Skipping large file: {full_path} ({file_size_mb:.1f} MB)")
        return json_files
    
    def load_json_file(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """Load JSON data from file, handling both single objects and arrays"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                
                # Handle empty files
                if not content:
                    logger.warning(f"Empty file: {file_path}")
                    return
                
                # Try to parse as JSON
                try:
                    data = json.loads(content)
                except json.JSONDecodeError:
                    # If JSON parsing fails, try JSONL format (one JSON object per line)
                    logger.info(f"Trying JSONL format for {file_path}")
                    for line_num, line in enumerate(content.split('\n'), 1):
                        line = line.strip()
                        if line:
                            try:
                                yield json.loads(line)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Error parsing line {line_num} in {file_path}: {e}")
                    return
                
                # Handle parsed JSON data
                if isinstance(data, list):
                    # File contains an array of objects
                    for item in data:
                        if isinstance(item, dict):
                            yield item
                        else:
                            logger.warning(f"Skipping non-object item in {file_path}: {type(item)}")
                elif isinstance(data, dict):
                    # File contains a single object
                    yield data
                else:
                    logger.warning(f"Unexpected data type in {file_path}: {type(data)}")
                    
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
    
    def bulk_index_documents(self, documents: List[Dict[str, Any]]) -> bool:
        """Bulk index documents to Elasticsearch"""
        if not documents:
            return True
        
        actions = []
        for doc in documents:
            action = {
                "_index": config.ELASTICSEARCH_INDEX,
                "_source": doc
            }
            # Use document ID if available
            if 'id' in doc:
                action["_id"] = doc['id']
            elif '_id' in doc:
                action["_id"] = doc['_id']
            
            actions.append(action)
        
        try:
            success, failed = helpers.bulk(
                self.es_client,
                actions,
                chunk_size=config.BATCH_SIZE,
                request_timeout=60
            )
            
            if failed:
                logger.warning(f"Failed to index {len(failed)} documents")
                for fail in failed[:3]:  # Log first 3 failures
                    logger.warning(f"Failed document: {fail}")
            
            logger.info(f"Successfully indexed {success} documents")
            return len(failed) == 0
            
        except Exception as e:
            logger.error(f"Error during bulk indexing: {e}")
            return False
    
    def process_file(self, file_path: str) -> int:
        """Process a single JSON file and return number of documents indexed"""
        logger.info(f"Processing file: {os.path.basename(file_path)}")
        
        documents = []
        doc_count = 0
        
        for doc in self.load_json_file(file_path):
            documents.append(doc)
            doc_count += 1
            
            # Bulk index when batch is full
            if len(documents) >= config.BATCH_SIZE:
                if self.bulk_index_documents(documents):
                    documents = []
                else:
                    logger.error(f"Failed to index batch from {file_path}")
                    return 0
        
        # Index remaining documents
        if documents:
            if not self.bulk_index_documents(documents):
                logger.error(f"Failed to index final batch from {file_path}")
                return 0
        
        logger.info(f"Processed {doc_count} documents from {os.path.basename(file_path)}")
        return doc_count
    
    def run(self):
        """Main execution method"""
        logger.info("Starting DSFP Data Loader (Local Data Directory Mode)")
        
        # Check if Elasticsearch index exists
        if not self.es_client.indices.exists(index=config.ELASTICSEARCH_INDEX):
            logger.error(f"Elasticsearch index '{config.ELASTICSEARCH_INDEX}' does not exist")
            logger.info("Please ensure the init-elasticsearch container has run successfully")
            sys.exit(1)
        
        # Check for data directory
        if not self.check_data_directory():
            sys.exit(1)
        
        total_docs = 0
        processed_files = 0
        
        # Find all JSON files in the data directory
        json_files = self.find_json_files(config.LOCAL_DATA_PATH)
        
        if not json_files:
            logger.error("No JSON files found in the data directory")
            sys.exit(1)
        
        logger.info(f"Processing {len(json_files)} JSON files...")
        
        # Process each JSON file
        for json_file in json_files:
            relative_path = os.path.relpath(json_file, config.LOCAL_DATA_PATH)
            logger.info(f"Processing: {relative_path}")
            docs = self.process_file(json_file)
            total_docs += docs
            if docs > 0:
                processed_files += 1
        
        logger.info(f"Data loading completed!")
        logger.info(f"Files processed: {processed_files}/{len(json_files)}")
        logger.info(f"Total documents indexed: {total_docs}")
        
        # Print final Elasticsearch stats
        try:
            stats = self.es_client.indices.stats(index=config.ELASTICSEARCH_INDEX)
            doc_count = stats['indices'][config.ELASTICSEARCH_INDEX]['total']['docs']['count']
            logger.info(f"Elasticsearch index now contains {doc_count} documents")
        except Exception as e:
            logger.warning(f"Could not retrieve final document count: {e}")


if __name__ == "__main__":
    loader = DataLoader()
    loader.run()
