-- ============================================================
-- BellosData Lake â€” DuckDB Query Console
-- ============================================================
-- Run with:  duckdb < query.sql
-- Or interactive:  duckdb  (then paste queries)
-- Cost: $0 forever. Reads S3 Delta tables directly.
-- ============================================================

-- 1. Install extensions (only needed once)
INSTALL httpfs;
INSTALL delta;
LOAD httpfs;
LOAD delta;

-- 2. Configure AWS credentials
-- Uses your local AWS SSO credentials automatically
SET s3_region = 'eu-west-2';

-- If SSO doesn't auto-resolve, set these from your .env:
-- SET s3_access_key_id = 'YOUR_KEY';
-- SET s3_secret_access_key = 'YOUR_SECRET';

-- ============================================================
-- BRONZE LAYER (RDL) â€” Raw ingested data
-- ============================================================

-- Preview weather data
SELECT * FROM delta_scan('s3://bellosdata-bronze-raw/rdl/weather/') LIMIT 10;

-- Preview wind data
SELECT * FROM delta_scan('s3://bellosdata-bronze-raw/rdl/wind/') LIMIT 10;

-- Count records per source
SELECT 'weather' AS source, COUNT(*) AS records FROM delta_scan('s3://bellosdata-bronze-raw/rdl/weather/')
UNION ALL
SELECT 'wind', COUNT(*) FROM delta_scan('s3://bellosdata-bronze-raw/rdl/wind/');

-- ============================================================
-- SILVER LAYER (ODL) â€” Typed dimensions, facts, mappings
-- (These populate after DAGs run)
-- ============================================================

-- Dimensions
-- SELECT * FROM delta_scan('s3://bellosdata-silver-curated/odl/dim/dim_date/') LIMIT 10;
-- SELECT * FROM delta_scan('s3://bellosdata-silver-curated/odl/dim/dim_location/') LIMIT 10;
-- SELECT * FROM delta_scan('s3://bellosdata-silver-curated/odl/dim/dim_weather_station/') LIMIT 10;
-- SELECT * FROM delta_scan('s3://bellosdata-silver-curated/odl/dim/dim_airport/') LIMIT 10;

-- Facts
-- SELECT * FROM delta_scan('s3://bellosdata-silver-curated/odl/fact/fact_weather_observation/') LIMIT 10;
-- SELECT * FROM delta_scan('s3://bellosdata-silver-curated/odl/fact/fact_company_filing/') LIMIT 10;

-- Mappings
-- SELECT * FROM delta_scan('s3://bellosdata-silver-curated/odl/map/map_company_to_postcode/') LIMIT 10;
-- SELECT * FROM delta_scan('s3://bellosdata-silver-curated/odl/map/map_airport_to_location/') LIMIT 10;

-- ============================================================
-- EXAMPLE JOINS (uncomment after DAGs populate Silver layer)
-- ============================================================

-- Weather observations joined to station info
-- SELECT
--     f.observation_timestamp,
--     f.temperature_2m_c,
--     f.precipitation_mm,
--     f.wind_speed_10m_ms,
--     m.nearest_postcode,
--     m.distance_km
-- FROM delta_scan('s3://bellosdata-silver-curated/odl/fact/fact_weather_observation/') f
-- JOIN delta_scan('s3://bellosdata-silver-curated/odl/map/map_station_to_location/') m
--   ON f.station_point_id = m.point_id
-- ORDER BY f.observation_timestamp DESC
-- LIMIT 20;

-- NW Companies by SIC sector
-- SELECT
--     sic_primary,
--     COUNT(*) AS company_count,
--     AVG(company_age_years) AS avg_age
-- FROM delta_scan('s3://bellosdata-silver-curated/odl/map/map_company_to_postcode/')
-- GROUP BY sic_primary
-- ORDER BY company_count DESC
-- LIMIT 20;
