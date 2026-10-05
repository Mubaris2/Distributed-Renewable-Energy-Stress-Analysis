-- schema.sql
-- Hive external tables over the Pig ETL output.
-- External tables so dropping the table never deletes the underlying HDFS
-- data, Pig remains the source of truth for this layer.

CREATE DATABASE IF NOT EXISTS energy_db;
USE energy_db;

CREATE EXTERNAL TABLE IF NOT EXISTS readings (
    country                   STRING,
    raw_timestamp             STRING,
    demand_mw                 DOUBLE,
    solar_mw                  DOUBLE,
    wind_onshore_mw           DOUBLE,
    wind_offshore_mw          DOUBLE,
    temperature_c             DOUBLE,
    humidity_pct              DOUBLE,
    wind_speed_ms             DOUBLE,
    hour                      INT,
    renewable_generation_mw   DOUBLE,
    net_load_mw               DOUBLE,
    renewable_share           DOUBLE,
    grid_stress_indicator     DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/big_data/distributed_energy/processed/nl_clean_integrated';

CREATE EXTERNAL TABLE IF NOT EXISTS flagged_solar_gaps (
    country           STRING,
    raw_timestamp     STRING,
    demand_mw         DOUBLE,
    solar_mw          DOUBLE,
    wind_onshore_mw   DOUBLE,
    wind_offshore_mw  DOUBLE,
    temperature_c     DOUBLE,
    humidity_pct      DOUBLE,
    wind_speed_ms     DOUBLE,
    hour              INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/big_data/distributed_energy/processed/flagged_solar_gaps';

-- Sanity check: row counts per table, and per country within readings
SELECT 'readings' AS table_name, COUNT(*) AS row_count FROM readings
UNION ALL
SELECT 'flagged_solar_gaps' AS table_name, COUNT(*) AS row_count FROM flagged_solar_gaps;

SELECT country, COUNT(*) AS row_count FROM readings GROUP BY country;