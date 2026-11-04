CREATE TABLE telemetry_table (
    id SERIAL PRIMARY KEY,
    buoy_id VARCHAR(50) NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    sea_surface_temperature DOUBLE PRECISION NOT NULL,
    -- Stored Generated Column computes PostGIS geometry automatically during bulk COPY streams
    geom GEOMETRY(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
    ) STORED
);

-- Index the spatial column to ensure lightning-fast downstream ERDDAP queries
CREATE INDEX idx_telemetry_geom ON telemetry_table USING GIST (geom);
