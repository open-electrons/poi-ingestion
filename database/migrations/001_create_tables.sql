-- 001_create_tables.sql

-- 1️⃣ POIs table first
CREATE TABLE IF NOT EXISTS pois (
    id BIGINT PRIMARY KEY,
    country_code VARCHAR(10),
    title TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 2️⃣ Connections table next
CREATE TABLE IF NOT EXISTS connections (
    id BIGINT PRIMARY KEY,
    poi_id BIGINT NOT NULL REFERENCES pois(id) ON DELETE CASCADE,
    connection_type TEXT,
    power_kw DOUBLE PRECISION,
    voltage INTEGER,
    amperage INTEGER,
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 3️⃣ Indexes
CREATE INDEX IF NOT EXISTS idx_pois_country ON pois(country_code);
CREATE INDEX IF NOT EXISTS idx_connections_poi ON connections(poi_id);
