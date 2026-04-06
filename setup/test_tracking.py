#!/usr/bin/env python3
"""
Test script for DuckDB tracking implementation
"""

import sys
import os
sys.path.append('/app/setup')

from tracking_db import TrackingDatabase
from datetime import datetime
import json

def test_tracking_db():
    print("Testing DuckDB tracking implementation...")
    
    # Initialize database
    db = TrackingDatabase(db_path="/data/test_tracking.duckdb", parquet_dir="/data/test_parquet")
    
    # Test data
    test_screening = {
        "screening_id": "test_sample_123_caffeine-cocaine_20241106_120000",
        "sample_id": "123",
        "collection_id": 1,
        "substances_screened": ["caffeine", "cocaine"],
        "last_screened": datetime.utcnow().isoformat() + 'Z',
        "screening_request": {
            "mz_tolerance": 0.01,
            "rti_tolerance": 0.1,
            "filter_by_blanks": True,
            "substances": ["caffeine", "cocaine"]
        },
        "screening_results": {
            "results": [
                {
                    "substance_id": "caffeine",
                    "sample_id": "123",
                    "collection_id": 1,
                    "collection_uid": "test_collection",
                    "collection_title": "Test Collection",
                    "short_name": "Test Sample 123",
                    "matrix_type": "wastewater",
                    "sample_type": "influent",
                    "monitored_city": "Test City",
                    "sampling_date": "2024-11-06",
                    "analysis_date": "2024-11-06",
                    "latitude": 52.5200,
                    "longitude": 13.4050,
                    "detection_id": "det_001",
                    "instrument_setup_used": {
                        "setup_id": "setup_001",
                        "instrument": "LCMS",
                        "column": "C18",
                        "ionization": "ESI"
                    },
                    "scores": {
                        "rti": 0.9,
                        "mz": 0.95,
                        "fragments": 0.8,
                        "spectral_similarity": 0.85,
                        "isotopic_fit": 0.75,
                        "molecular_formula_fit": 0.9,
                        "ip_score": 0.82
                    },
                    "semiquantification": {
                        "method": "external_standard",
                        "concentration": 125.5
                    }
                },
                {
                    "substance_id": "cocaine",
                    "sample_id": "123",
                    "collection_id": 1,
                    "short_name": "Test Sample 123",
                    "scores": {
                        "rti": 0.7,
                        "mz": 0.88,
                        "spectral_similarity": 0.65
                    }
                }
            ]
        }
    }
    
    # Test save
    print("1. Testing save_screening_result...")
    success = db.save_screening_result(test_screening)
    print(f"   Result: {'SUCCESS' if success else 'FAILED'}")
    
    # Test query
    print("2. Testing get_tracking_files...")
    files = db.get_tracking_files()
    print(f"   Found {len(files)} files:")
    for file_data in files:
        print(f"   - Sample {file_data['sample_id']}: {file_data['short_name']}")
        print(f"     Substances screened: {file_data['substances_screened']}")
        print(f"     Last screened: {file_data['last_screened']}")
    
    # Test direct SQL queries
    print("3. Testing direct database queries...")
    try:
        tracking_count = db.conn.execute("SELECT COUNT(*) FROM screening_tracking").fetchone()[0]
        results_count = db.conn.execute("SELECT COUNT(*) FROM screening_results").fetchone()[0]
        print(f"   Tracking records: {tracking_count}")
        print(f"   Results records: {results_count}")
        
        # Show sample data
        sample_data = db.conn.execute("""
            SELECT screening_id, sample_id, array_length(substances_screened) as num_substances, last_screened 
            FROM screening_tracking 
            LIMIT 5
        """).fetchall()
        
        print("   Sample tracking data:")
        for row in sample_data:
            print(f"   - {row[0]}: Sample {row[1]}, {row[2]} substances, {row[3]}")
            
    except Exception as e:
        print(f"   Database query error: {e}")
    
    print("4. Testing Parquet export...")
    try:
        db._export_to_parquet()
        
        # Check if parquet files exist
        import os
        parquet_dir = "/data/test_parquet"
        tracking_file = os.path.join(parquet_dir, "screening_tracking.parquet")
        results_file = os.path.join(parquet_dir, "screening_results.parquet")
        
        if os.path.exists(tracking_file) and os.path.exists(results_file):
            print("   Parquet export: SUCCESS")
            print(f"   Files created: {tracking_file}, {results_file}")
        else:
            print("   Parquet export: FAILED - files not found")
            
    except Exception as e:
        print(f"   Parquet export error: {e}")
    
    db.close()
    print("Test completed!")

if __name__ == "__main__":
    test_tracking_db()
