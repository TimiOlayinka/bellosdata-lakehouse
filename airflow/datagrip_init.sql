-- ============================================================
-- BellosData Lake — DataGrip Init Script
-- ============================================================
-- Add this as a "Startup script" in your DataGrip DuckDB connection.
-- It runs automatically every time you connect.
--
-- SETUP: Replace YOUR_ACCESS_KEY and YOUR_SECRET_KEY with your
--        AWS credentials before using.
-- ============================================================

INSTALL httpfs;
LOAD httpfs;

SET s3_region = 'eu-west-2';
SET s3_access_key_id = 'YOUR_ACCESS_KEY';
SET s3_secret_access_key = 'YOUR_SECRET_KEY';

-- ============================================================
-- BRONZE LAYER (RDL) — Raw ingested data
-- ============================================================
CREATE OR REPLACE VIEW weather    AS SELECT * FROM read_parquet('s3://playdarch-bronze-raw/rdl/weather/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW wind       AS SELECT * FROM read_parquet('s3://playdarch-bronze-raw/rdl/wind/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW postcodes  AS SELECT * FROM read_parquet('s3://playdarch-bronze-raw/rdl/postcodes/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW airports   AS SELECT * FROM read_parquet('s3://playdarch-bronze-raw/rdl/airports/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW companies  AS SELECT * FROM read_parquet('s3://playdarch-bronze-raw/rdl/companies/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW landscapes AS SELECT * FROM read_parquet('s3://playdarch-bronze-raw/rdl/landscapes/**/*.parquet', hive_partitioning=true);

-- ============================================================
-- SILVER LAYER — Dimensions
-- ============================================================
CREATE OR REPLACE VIEW dim_date            AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/dim/dim_date/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_location        AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/dim/dim_location/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_weather_station AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/dim/dim_weather_station/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_airport         AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/dim/dim_airport/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_habitat         AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/dim/dim_habitat/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_company         AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/dim/dim_company/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_species         AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/dim/dim_species/**/*.parquet', hive_partitioning=true);

-- ============================================================
-- SILVER LAYER — Facts
-- ============================================================
CREATE OR REPLACE VIEW fact_weather  AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/fact/fact_weather_observation/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW fact_wind     AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/fact/fact_wind_measurement/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW fact_flight   AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/fact/fact_flight_movement/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW fact_company  AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/fact/fact_company_filing/**/*.parquet', hive_partitioning=true);

-- ============================================================
-- SILVER LAYER — Mappings
-- ============================================================
CREATE OR REPLACE VIEW map_postcode_location AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/map/map_postcode_to_location/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_company_postcode  AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/map/map_company_to_postcode/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_airport_location  AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/map/map_airport_to_location/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_species_habitat   AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/map/map_species_to_habitat/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_owner_company     AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/map/map_owner_to_company/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_station_location  AS SELECT * FROM read_parquet('s3://playdarch-silver-curated/odl/map/map_station_to_location/**/*.parquet', hive_partitioning=true);
