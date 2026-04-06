-- DuckDB schema for screening tracking
-- This replaces the Elasticsearch tracking index

-- Create main tracking table
CREATE TABLE IF NOT EXISTS screening_tracking (
    screening_id VARCHAR PRIMARY KEY,
    sample_id VARCHAR NOT NULL,
    collection_id BIGINT,
    substances_screened VARCHAR[], -- Array of substance names
    last_screened TIMESTAMP NOT NULL,
    -- Screening request parameters
    mz_tolerance DOUBLE,
    rti_tolerance DOUBLE,
    filter_by_blanks BOOLEAN,
    -- Results metadata
    total_results INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create screening results table (flattened from nested ES structure)
CREATE TABLE IF NOT EXISTS screening_results (
    result_id VARCHAR PRIMARY KEY, -- screening_id + substance_id + sample_id
    screening_id VARCHAR NOT NULL,
    substance_id VARCHAR,
    sample_id VARCHAR,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (screening_id) REFERENCES screening_tracking(screening_id)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_tracking_sample_id ON screening_tracking(sample_id);
CREATE INDEX IF NOT EXISTS idx_tracking_last_screened ON screening_tracking(last_screened);
CREATE INDEX IF NOT EXISTS idx_results_screening_id ON screening_results(screening_id);
CREATE INDEX IF NOT EXISTS idx_results_sample_id ON screening_results(sample_id);
CREATE INDEX IF NOT EXISTS idx_results_substance_id ON screening_results(substance_id);

-- Create summary view for API consumption
CREATE OR REPLACE VIEW tracking_summary AS
SELECT 
    s.sample_id,
    s.short_name,
    COALESCE(array_length(t.substances_screened), 0) as substances_screened,
    t.last_screened,
    t.screening_id,
    COUNT(r.result_id) as total_detections
FROM (
    -- Get unique samples from screening index (this will be replaced with actual sample data)
    SELECT DISTINCT sample_id, short_name FROM screening_results
) s
LEFT JOIN (
    -- Get latest screening per sample
    SELECT DISTINCT ON (sample_id) 
        sample_id, 
        substances_screened, 
        last_screened, 
        screening_id
    FROM screening_tracking 
    ORDER BY sample_id, last_screened DESC
) t ON s.sample_id = t.sample_id
LEFT JOIN screening_results r ON t.screening_id = r.screening_id
GROUP BY s.sample_id, s.short_name, t.substances_screened, t.last_screened, t.screening_id
ORDER BY s.sample_id;
