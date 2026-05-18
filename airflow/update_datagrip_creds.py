import boto3

session = boto3.Session(profile_name="playEngineer", region_name="eu-west-2")
creds = session.get_credentials().get_frozen_credentials()

sql = f"""INSTALL httpfs;
LOAD httpfs;

SET s3_region = 'eu-west-2';
SET s3_access_key_id = '{creds.access_key}';
SET s3_secret_access_key = '{creds.secret_key}';
SET s3_session_token = '{creds.token}';
SET s3_endpoint = 's3.eu-west-2.amazonaws.com';
SET s3_url_style = 'vhost';
SET s3_use_ssl = false;

-- ============================================================
-- BRONZE LAYER (RDL)
-- ============================================================
CREATE OR REPLACE VIEW weather    AS SELECT * FROM read_parquet('s3://bellosdata-bronze-raw/rdl/weather/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW wind       AS SELECT * FROM read_parquet('s3://bellosdata-bronze-raw/rdl/wind/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW postcodes  AS SELECT * FROM read_parquet('s3://bellosdata-bronze-raw/rdl/postcodes/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW airports   AS SELECT * FROM read_parquet('s3://bellosdata-bronze-raw/rdl/airports/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW companies  AS SELECT * FROM read_parquet('s3://bellosdata-bronze-raw/rdl/companies/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW landscapes AS SELECT * FROM read_parquet('s3://bellosdata-bronze-raw/rdl/landscapes/**/*.parquet', hive_partitioning=true);

-- ============================================================
-- SILVER LAYER â€” Dimensions
-- ============================================================
CREATE OR REPLACE VIEW dim_date            AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/dim/dim_date/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_location        AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/dim/dim_location/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_weather_station AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/dim/dim_weather_station/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_airport         AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/dim/dim_airport/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_habitat         AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/dim/dim_habitat/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_company         AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/dim/dim_company/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW dim_species         AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/dim/dim_species/**/*.parquet', hive_partitioning=true);

-- ============================================================
-- SILVER LAYER â€” Facts
-- ============================================================
CREATE OR REPLACE VIEW fact_weather  AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/fact/fact_weather_observation/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW fact_wind     AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/fact/fact_wind_measurement/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW fact_flight   AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/fact/fact_flight_movement/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW fact_company  AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/fact/fact_company_filing/**/*.parquet', hive_partitioning=true);

-- ============================================================
-- SILVER LAYER â€” Mappings
-- ============================================================
CREATE OR REPLACE VIEW map_postcode_location AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/map/map_postcode_to_location/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_company_postcode  AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/map/map_company_to_postcode/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_airport_location  AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/map/map_airport_to_location/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_species_habitat   AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/map/map_species_to_habitat/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_owner_company     AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/map/map_owner_to_company/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW map_station_location  AS SELECT * FROM read_parquet('s3://bellosdata-silver-curated/odl/map/map_station_to_location/**/*.parquet', hive_partitioning=true);
"""

with open("datagrip_init.sql", "w") as f:
    f.write(sql)

print("Updated datagrip_init.sql with fresh SSO credentials and all view definitions.")
