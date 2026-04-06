#!/usr/bin/env python3
"""
DuckDB Tracking Database Manager
Replaces Elasticsearch-based tracking with DuckDB + Parquet storage
"""

import os
import duckdb
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrackingDatabase:
    def __init__(self, db_path: str = "/data/tracking.duckdb", parquet_dir: str = "/data/parquet"):
        """Initialize DuckDB tracking database"""
        self.db_path = db_path
        self.parquet_dir = parquet_dir
        self.conn = None
        
        # Ensure directories exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        os.makedirs(parquet_dir, exist_ok=True)
        
        # Initialize database
        self._initialize_db()
    
    def _initialize_db(self):
        """Initialize DuckDB connection and create tables"""
        try:
            self.conn = duckdb.connect(self.db_path)
            
            # Load schema
            schema_path = "/app/setup/tracking-schema.sql"
            if os.path.exists(schema_path):
                with open(schema_path, 'r') as f:
                    schema_sql = f.read()
                
                # DuckDB doesn't have executescript, so split and execute statements
                statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
                for statement in statements:
                    if statement:
                        self.conn.execute(statement)
                logger.info("Database schema initialized successfully")
            else:
                logger.warning(f"Schema file not found at {schema_path}, creating basic schema")
                self._create_basic_schema()
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _create_basic_schema(self):
        """Create basic schema if schema file is not available"""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS screening_tracking (
            sample_id VARCHAR NOT NULL,
            short_name VARCHAR,
            collection_id BIGINT,
            substances_screened INTEGER DEFAULT 0,
            total_screened INTEGER DEFAULT 0,
            last_screened TIMESTAMP,
            status VARCHAR DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (sample_id)
        );
        
        CREATE TABLE IF NOT EXISTS screening_results (
            result_id VARCHAR PRIMARY KEY,
            screening_id VARCHAR,
            sample_id VARCHAR,
            substance_id VARCHAR,
            substance_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data TEXT
        );
        """
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        for statement in statements:
            if statement:
                self.conn.execute(statement)
        logger.info("Basic schema created")
    
    def initialize_sample_if_needed(self, sample_id, short_name):
        """Initialize a sample in the tracking database if it doesn't exist"""
        try:
            # Check if sample already exists
            result = self.conn.execute("""
                SELECT sample_id FROM screening_tracking 
                WHERE sample_id = ?
            """, (sample_id,)).fetchone()
            
            if not result:
                # Insert new sample with default values
                self.conn.execute("""
                    INSERT INTO screening_tracking (
                        sample_id, short_name, collection_id, 
                        substances_screened, total_screened, 
                        last_screened, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    sample_id,
                    short_name,
                    None,  # collection_id - will be updated later
                    0,     # substances_screened
                    0,     # total_screened
                    None,  # last_screened
                    'pending'  # status
                ))
                print(f"Initialized sample: {sample_id}")
            return True
        except Exception as e:
            print(f"Error initializing sample {sample_id}: {e}")
            return False

    def save_screening_result(self, result_data):
        """Save a screening result to DuckDB"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO screening_results (
                    result_id, screening_id, sample_id, substance_id, 
                    created_at, data
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                result_data['result_id'],
                result_data.get('screening_id'),
                result_data.get('sample_id'), 
                result_data.get('substance_id'),
                result_data.get('created_at'),
                json.dumps(result_data)
            ))
            print(f"Saved result: {result_data['result_id']}")
            return True
        except Exception as e:
            print(f"Error saving result: {e}")
            return False

    def initialize_from_elasticsearch(self, es_url: str = "http://elasticsearch:9200"):
        """Initialize tracking database from Elasticsearch data"""
        try:
            import requests
            
            # Get unique samples from Elasticsearch screening index
            query = {
                "size": 0,
                "aggs": {
                    "unique_samples": {
                        "terms": {
                            "field": "data.sample_id.keyword",
                            "size": 10000
                        },
                        "aggs": {
                            "sample_info": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": ["data.sample_id", "data.short_name", "data.collection_id"]
                                }
                            }
                        }
                    }
                }
            }
            
            response = requests.post(f"{es_url}/dsfp-screening-index/_search", 
                                   json=query, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            samples_initialized = 0
            if 'aggregations' in data and 'unique_samples' in data['aggregations']:
                for bucket in data['aggregations']['unique_samples']['buckets']:
                    sample_id = bucket['key']
                    if bucket['sample_info']['hits']['hits']:
                        hit = bucket['sample_info']['hits']['hits'][0]['_source']['data']
                        
                        # Initialize sample in tracking
                        if self.initialize_sample_if_needed(str(sample_id), hit.get('short_name', 'Unknown')):
                            samples_initialized += 1
            
            logger.info(f"Initialized tracking database with {samples_initialized} samples from Elasticsearch")
            return samples_initialized
            
        except Exception as e:
            logger.error(f"Failed to initialize from Elasticsearch: {e}")
            return 0

    def get_tracking_files(self) -> List[Dict]:
        """Get tracking summary for all files"""
        try:
            query = """
            SELECT 
                sample_id,
                short_name,
                substances_screened,
                total_screened,
                last_screened,
                status
            FROM screening_tracking
            ORDER BY sample_id
            """
            
            result = self.conn.execute(query).fetchall()
            columns = ['sample_id', 'short_name', 'substances_screened', 'total_screened', 'last_screened', 'status']
            
            files = []
            for row in result:
                files.append(dict(zip(columns, row)))
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to get tracking files: {e}")
            return []
    
    def get_sample_status(self, sample_id: str) -> Dict:
        """Get detailed status for a specific sample"""
        try:
            # Get tracking info
            tracking = self.conn.execute("""
                SELECT * FROM screening_tracking WHERE sample_id = ?
            """, (sample_id,)).fetchone()
            
            if not tracking:
                return {"error": "Sample not found"}
            
            # Get results count
            results_count = self.conn.execute("""
                SELECT COUNT(*) FROM screening_results WHERE sample_id = ?
            """, (sample_id,)).fetchone()[0]
            
            # Convert to dict
            columns = ['sample_id', 'short_name', 'collection_id', 'substances_screened', 
                      'total_screened', 'last_screened', 'status', 'created_at']
            tracking_dict = dict(zip(columns, tracking))
            tracking_dict['total_results'] = results_count
            
            return tracking_dict
            
        except Exception as e:
            logger.error(f"Failed to get sample status: {e}")
            return {"error": str(e)}
    
    def _export_to_parquet(self):
        """Export tables to Parquet files for backup and analysis"""
        try:
            # Export tracking table
            tracking_df = self.conn.execute("SELECT * FROM screening_tracking").df()
            tracking_parquet = os.path.join(self.parquet_dir, "screening_tracking.parquet")
            tracking_df.to_parquet(tracking_parquet, index=False)
            
            # Export results table
            results_df = self.conn.execute("SELECT * FROM screening_results").df()
            results_parquet = os.path.join(self.parquet_dir, "screening_results.parquet")
            results_df.to_parquet(results_parquet, index=False)
            
            logger.info("Exported tables to Parquet files")
            
        except Exception as e:
            logger.error(f"Failed to export to Parquet: {e}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

# Global instance
tracking_db = None

def get_tracking_db() -> TrackingDatabase:
    """Get global tracking database instance"""
    global tracking_db
    if tracking_db is None:
        tracking_db = TrackingDatabase()
    return tracking_db

def initialize_tracking_db():
    """Initialize tracking database on startup"""
    global tracking_db
    tracking_db = TrackingDatabase()
    logger.info("Tracking database initialized")

if __name__ == "__main__":
    # Test the database
    db = TrackingDatabase()
    
    # Try to initialize from Elasticsearch
    files = db.get_tracking_files()
    if len(files) == 0:
        print("No tracking data found, initializing from Elasticsearch...")
        count = db.initialize_from_elasticsearch("http://elasticsearch:9200")
        print(f"Initialized with {count} samples from Elasticsearch")
        files = db.get_tracking_files()
    
    print(f"Query test: Found {len(files)} files")
    for i, file in enumerate(files[:5]):  # Show first 5
        print(f"  - {file}")
    if len(files) > 5:
        print(f"  ... and {len(files) - 5} more samples")
    
    db.close()
