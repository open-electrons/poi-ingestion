-- 001_create_tables.sql

-- POIs table
CREATE TABLE IF NOT EXISTS pois (
    poi_uuid TEXT PRIMARY KEY,
    country_code VARCHAR(2),
    title TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Connections table
CREATE TABLE IF NOT EXISTS connections (
    id BIGSERIAL PRIMARY KEY,
    poi_uuid TEXT NOT NULL REFERENCES pois(poi_uuid) ON DELETE CASCADE,
    connection_id BIGINT,
    connection_type TEXT,
    power_kw DOUBLE PRECISION,
    voltage INTEGER,
    amperage INTEGER,
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (poi_uuid, connection_id)
);

-- 3️⃣ Indexes
CREATE INDEX IF NOT EXISTS idx_pois_country ON pois(country_code);
CREATE INDEX IF NOT EXISTS idx_connections_poi ON connections(poi_uuid);
