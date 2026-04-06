-- DuckDB schema for screening tracking
-- This replaces the Elasticsearch tracking index

-- Create main tracking table
-- One row per sample, tracks when the sample was last screened
CREATE TABLE IF NOT EXISTS screening_tracking (
    sample_id VARCHAR NOT NULL PRIMARY KEY,
    short_name VARCHAR,  -- Human-readable name for the sample
    collection_id BIGINT,
    last_screened TIMESTAMP NOT NULL,
    last_substance_screened VARCHAR,  -- Name of the last substance that was screened
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create screening results table (flattened from nested ES structure)
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
    -- Instrument setup
    setup_id VARCHAR,
    instrument VARCHAR,
    column_type VARCHAR,
    ionization VARCHAR,
    -- Screening request parameters (moved from tracking)
    mz_tolerance DOUBLE,
    rti_tolerance DOUBLE,
    filter_by_blanks BOOLEAN,
    -- Scores
    rti_score DOUBLE,
    mz_score DOUBLE,
    fragments_score DOUBLE,
    spectral_similarity_score DOUBLE,
    isotopic_fit_score DOUBLE,
    molecular_formula_fit_score DOUBLE,
    ip_score DOUBLE,
    -- Semiquantification
    semiquant_method VARCHAR,
    concentration DOUBLE,
    -- Matching inner hits from Elasticsearch
    matches JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Composite primary key to ensure only one result per sample-substance
    PRIMARY KEY (sample_id, substance_name)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_tracking_sample_id ON screening_tracking(sample_id);
CREATE INDEX IF NOT EXISTS idx_tracking_last_screened ON screening_tracking(last_screened);
CREATE INDEX IF NOT EXISTS idx_results_sample_id ON screening_results(sample_id);
CREATE INDEX IF NOT EXISTS idx_results_substance_name ON screening_results(substance_name);

-- Create summary view for API consumption
-- Drop existing view first to ensure we have the latest definition
DROP VIEW IF EXISTS tracking_summary;

CREATE VIEW tracking_summary AS
SELECT 
    t.sample_id,
    t.short_name,
    COUNT(DISTINCT r.substance_name) as substances_screened,
    COUNT(*) as total_results,
    t.last_screened,
    COUNT(CASE WHEN r.spectral_similarity_score > 0.7 THEN 1 END) as total_detections
FROM screening_tracking t
LEFT JOIN screening_results r ON t.sample_id = r.sample_id
GROUP BY t.sample_id, t.short_name, t.last_screened
ORDER BY t.sample_id;
