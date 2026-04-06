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
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure connection is closed"""
        self.close()
        return False
    
    def close(self):
        """Explicitly close database connection"""
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
    
    def _initialize_db(self):
        """Initialize DuckDB connection and create tables"""
        try:
            # Close existing connection if any
            if self.conn:
                self.conn.close()
            
            # Open connection with retry on lock
            max_retries = 3
            retry_delay = 0.5
            
            for attempt in range(max_retries):
                try:
                    self.conn = duckdb.connect(self.db_path)
                    break
                except Exception as e:
                    if attempt < max_retries - 1 and "lock" in str(e).lower():
                        logger.warning(f"Database locked, retrying in {retry_delay}s... (attempt {attempt + 1}/{max_retries})")
                        import time
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise
            
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
        # Create tables separately to avoid foreign key issues
        
        # First create tracking table with primary key
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS screening_tracking (
                sample_id VARCHAR NOT NULL PRIMARY KEY,
                short_name VARCHAR,
                collection_id BIGINT,
                last_screened TIMESTAMP NOT NULL,
                last_substance_screened VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Then create results table with foreign key
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS screening_results (
                sample_id VARCHAR NOT NULL,
                substance_name VARCHAR NOT NULL,
                collection_id BIGINT,
                collection_uid VARCHAR,
                collection_title VARCHAR,
                short_name VARCHAR,
                matrix_type VARCHAR,
                matrix_type2 VARCHAR,
                sample_type VARCHAR,
                monitored_city VARCHAR,
                sampling_date DATE,
                analysis_date DATE,
                latitude DOUBLE,
                longitude DOUBLE,
                detection_id VARCHAR,
                setup_id VARCHAR,
                instrument VARCHAR,
                column_type VARCHAR,
                ionization VARCHAR,
                mz_tolerance DOUBLE,
                rti_tolerance DOUBLE,
                filter_by_blanks BOOLEAN,
                rti_score DOUBLE,
                mz_score DOUBLE,
                fragments_score DOUBLE,
                spectral_similarity_score DOUBLE,
                isotopic_fit_score DOUBLE,
                molecular_formula_fit_score DOUBLE,
                ip_score DOUBLE,
                semiquant_method VARCHAR,
                concentration DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (sample_id, substance_name),
                FOREIGN KEY (sample_id) REFERENCES screening_tracking(sample_id)
            )
        """)
    
    def save_screening_result(self, sample_id: str, substance_name: str, substance_id: str = None, result_data: Dict = None, timestamp: str = None) -> bool:
        """Save individual screening result to DuckDB (all fields, with explicit mapping and debug logging)"""
        try:
            self.initialize_sample_if_needed(sample_id)
            if not timestamp:
                timestamp = datetime.now().isoformat() + 'Z'
            # Defensive extraction
            result_data = result_data or {}
            scores = result_data.get('scores', {}) or {}
            instrument = result_data.get('instrument_setup_used', {}) or {}
            semiquant = result_data.get('semiquantification', {}) or {}
            matches = result_data.get('matches', [])
            
            # Explicit mapping for debug
            insert_values = [
                str(sample_id),
                substance_name,
                result_data.get('collection_id'),
                result_data.get('collection_uid'),
                result_data.get('collection_title'),
                result_data.get('short_name') or f'Sample {sample_id}',
                result_data.get('matrix_type'),
                result_data.get('matrix_type2'),
                result_data.get('sample_type'),
                result_data.get('monitored_city'),
                result_data.get('sampling_date'),
                result_data.get('analysis_date'),
                result_data.get('latitude'),
                result_data.get('longitude'),
                result_data.get('detection_id'),
                instrument.get('setup_id'),
                instrument.get('instrument'),
                instrument.get('column'),
                instrument.get('ionization'),
                result_data.get('mz_tolerance'),
                result_data.get('rti_tolerance'),
                result_data.get('filter_by_blanks'),
                scores.get('rti'),
                scores.get('mz'),
                scores.get('fragments'),
                scores.get('spectral_similarity'),
                scores.get('isotopic_fit'),
                scores.get('molecular_formula_fit'),
                scores.get('ip_score'),
                semiquant.get('method'),
                semiquant.get('concentration'),
                json.dumps(matches) if matches else None,
                timestamp
            ]
            logger.debug(f"Inserting screening_result: {insert_values}")
            self.conn.execute("""
                INSERT OR REPLACE INTO screening_results 
                (sample_id, substance_name, collection_id, collection_uid,
                 collection_title, short_name, matrix_type, matrix_type2, sample_type, 
                 monitored_city, sampling_date, analysis_date, latitude, longitude, detection_id,
                 setup_id, instrument, column_type, ionization, 
                 mz_tolerance, rti_tolerance, filter_by_blanks,
                 rti_score, mz_score, fragments_score, spectral_similarity_score, 
                 isotopic_fit_score, molecular_formula_fit_score, ip_score, 
                 semiquant_method, concentration, matches, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, insert_values)
            self.conn.commit()
            logger.info(f"Saved screening result: sample {sample_id}, substance {substance_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to save screening result: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    def save_screening_tracking(self, screening_data: Dict) -> bool:
        """Save screening tracking data and results (with explicit mapping and debug logging)"""
        try:
            sample_id = screening_data['sample_id']
            collection_id = screening_data.get('collection_id')
            last_screened = screening_data['last_screened']
            mz_tolerance = screening_data.get('screening_request', {}).get('mz_tolerance')
            rti_tolerance = screening_data.get('screening_request', {}).get('rti_tolerance')
            filter_by_blanks = screening_data.get('screening_request', {}).get('filter_by_blanks')
            results = screening_data.get('screening_results', {}).get('results', [])
            last_substance = None
            new_short_name = None
            if results:
                last_substance = results[-1].get('substance_name') or results[-1].get('substance_id')
                new_short_name = results[0].get('short_name')
            existing = self.conn.execute("""
                SELECT short_name FROM screening_tracking WHERE sample_id = ?
            """, [str(sample_id)]).fetchone()
            if existing:
                self.conn.execute("""
                    UPDATE screening_tracking SET last_screened = ?, last_substance_screened = ? WHERE sample_id = ?
                """, [last_screened, last_substance, str(sample_id)])
            else:
                self.conn.execute("""
                    INSERT INTO screening_tracking (sample_id, short_name, collection_id, last_screened, last_substance_screened)
                    VALUES (?, ?, ?, ?, ?)
                """, [str(sample_id), new_short_name, collection_id, last_screened, last_substance])
            for result in results:
                scores = result.get('scores', {}) or {}
                instrument = result.get('instrument_setup_used', {}) or {}
                semiquant = result.get('semiquantification', {}) or {}
                matches = result.get('matches', [])
                
                insert_values = [
                    str(sample_id),
                    result.get('substance_name') or result.get('substance_id'),
                    result.get('collection_id') or collection_id,
                    result.get('collection_uid'),
                    result.get('collection_title'),
                    result.get('short_name'),
                    result.get('matrix_type'),
                    result.get('matrix_type2'),
                    result.get('sample_type'),
                    result.get('monitored_city'),
                    result.get('sampling_date'),
                    result.get('analysis_date'),
                    result.get('latitude'),
                    result.get('longitude'),
                    result.get('detection_id'),
                    instrument.get('setup_id'),
                    instrument.get('instrument'),
                    instrument.get('column'),
                    instrument.get('ionization'),
                    mz_tolerance,
                    rti_tolerance,
                    filter_by_blanks,
                    scores.get('rti'),
                    scores.get('mz'),
                    scores.get('fragments'),
                    scores.get('spectral_similarity'),
                    scores.get('isotopic_fit'),
                    scores.get('molecular_formula_fit'),
                    scores.get('ip_score'),
                    semiquant.get('method'),
                    semiquant.get('concentration'),
                    json.dumps(matches) if matches else None
                ]
                logger.debug(f"Inserting screening_result (tracking): {insert_values}")
                self.conn.execute("""
                    INSERT OR REPLACE INTO screening_results 
                    (sample_id, substance_name, collection_id, collection_uid,
                     collection_title, short_name, matrix_type, matrix_type2, sample_type, 
                     monitored_city, sampling_date, analysis_date, latitude, longitude, detection_id,
                     setup_id, instrument, column_type, ionization, 
                     mz_tolerance, rti_tolerance, filter_by_blanks,
                     rti_score, mz_score, fragments_score, spectral_similarity_score, 
                     isotopic_fit_score, molecular_formula_fit_score, ip_score, 
                     semiquant_method, concentration, matches)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, insert_values)
            self.conn.commit()
            self._export_to_parquet()
            logger.info(f"Saved screening result: sample {sample_id}, {len(results)} substances")
            return True
        except Exception as e:
            logger.error(f"Failed to save screening result: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    def initialize_sample_if_needed(self, sample_id: str, short_name: str = None):
        """Initialize a sample in tracking if it doesn't exist"""
        try:
            # Check if sample already exists in tracking
            existing = self.conn.execute("""
                SELECT COUNT(*) FROM screening_tracking 
                WHERE sample_id = ?
            """, [sample_id]).fetchone()[0]
            
            if existing == 0:
                # Create initial tracking record
                self.conn.execute("""
                    INSERT INTO screening_tracking 
                    (sample_id, short_name, collection_id, last_screened, last_substance_screened)
                    VALUES (?, ?, ?, ?, ?)
                """, [
                    str(sample_id),
                    short_name,  # Now we store short_name
                    None,  # collection_id
                    datetime.now().isoformat() + 'Z',
                    None  # last_substance_screened
                ])
                
                # Force commit
                self.conn.commit()
                
                logger.info(f"Initialized tracking for sample {sample_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize sample {sample_id}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
    
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
                            "field": "sample_id",
                            "size": 10000
                        },
                        "aggs": {
                            "sample_info": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": ["sample_id", "short_name", "collection_id", "collection_title"]
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
                        hit = bucket['sample_info']['hits']['hits'][0]['_source']
                        
                        # Insert tracking record
                        self.conn.execute("""
                            INSERT OR REPLACE INTO screening_tracking 
                            (sample_id, short_name, collection_id, last_screened, last_substance_screened)
                            VALUES (?, ?, ?, ?, ?)
                        """, [
                            str(sample_id),
                            hit.get('short_name'),
                            hit.get('collection_id'),
                            datetime.now().isoformat() + 'Z',
                            None  # last_substance_screened
                        ])
                        
                        samples_initialized += 1
            
            # Export to Parquet
            self._export_to_parquet()
            
            logger.info(f"Initialized tracking database with {samples_initialized} samples from Elasticsearch")
            return samples_initialized
            
        except Exception as e:
            logger.error(f"Failed to initialize from Elasticsearch: {e}")
            return 0

    def get_tracking_files(self) -> List[Dict]:
        """Get tracking summary for all files (replaces /api/tracking/files)"""
        try:
            # Use the tracking_summary view which auto-calculates counts
            query = """
            SELECT 
                t.sample_id,
                t.short_name,
                COUNT(DISTINCT r.substance_name) as substances_screened,
                COUNT(*) as total_results,
                t.last_screened,
                COUNT(CASE WHEN r.spectral_similarity_score > 0.7 THEN 1 END) as substances_detected
            FROM screening_tracking t
            LEFT JOIN screening_results r ON t.sample_id = r.sample_id
            GROUP BY t.sample_id, t.short_name, t.last_screened
            ORDER BY t.sample_id
            """
            
            result = self.conn.execute(query).fetchall()
            columns = ['sample_id', 'short_name', 'substances_screened', 'total_results', 'last_screened', 'substances_detected']
            
            files = []
            for row in result:
                files.append(dict(zip(columns, row)))
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to get tracking files: {e}")
            return []
    
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
        """Close database connection properly"""
        if self.conn:
            try:
                # Ensure any pending transactions are committed
                self.conn.commit()
                self.conn.close()
                self.conn = None
                logger.info("Database connection closed properly")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
                # Force set to None even if close fails
                self.conn = None

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
    
    # Get existing tracking files
    files = db.get_tracking_files()
    print(f"Query test: Found {len(files)} tracked samples")
    for i, file in enumerate(files[:5]):  # Show first 5
        print(f"  - {file}")
    if len(files) > 5:
        print(f"  ... and {len(files) - 5} more samples")
    
    db.close()
