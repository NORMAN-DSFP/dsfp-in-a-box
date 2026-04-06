#!/usr/bin/env python3
"""
Migration script to add matches column to screening_results table
"""

import duckdb
import os

def add_matches_column():
    db_path = "/data/tracking.duckdb"
    
    if not os.path.exists(db_path):
        print("Database does not exist yet, skipping migration")
        return
    
    try:
        conn = duckdb.connect(db_path)
        
        # Check if column already exists
        result = conn.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'screening_results' 
            AND column_name = 'matches'
        """).fetchall()
        
        if len(result) > 0:
            print("Column 'matches' already exists, skipping migration")
            conn.close()
            return
        
        # Add the matches column
        print("Adding 'matches' column to screening_results table...")
        conn.execute("ALTER TABLE screening_results ADD COLUMN matches JSON")
        
        print("Migration completed successfully")
        conn.close()
        
    except Exception as e:
        print(f"Migration failed: {e}")
        raise

if __name__ == "__main__":
    add_matches_column()
