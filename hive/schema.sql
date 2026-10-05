-- schema.sql
-- Hive external tables over the Pig ETL output.
-- External tables so dropping the table never deletes the underlying HDFS
-- data, Pig remains the source of truth for this layer.

CREATE DATABASE IF NOT EXISTS energy_db;
USE energy_db;

-- Main clean, integrated dataset: one row per 15-min timestamp.
CREATE EXTERNAL TABLE IF NOT EXISTS nl_readings (
    raw_timestamp            STRING,
    demand_mw                DOUBLE,
    solar_mw                 DOUBLE,
    wind_onshore_mw          DOUBLE,
    wind_offshore_mw         DOUBLE,
    temperature_c            DOUBLE,
    humidity_pct             DOUBLE,
    wind_speed_ms            DOUBLE,
    hour                     INT,
    renewable_generation_mw  DOUBLE,
    net_load_mw              DOUBLE,
    renewable_share          DOUBLE,
    grid_stress_indicator    DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/big_data/distributed_energy/processed/nl_clean_integrated';

-- Rows where solar generation was missing during daylight hours (05:00-21:59)
-- and intentionally NOT imputed, kept for the veracity / data-quality
-- discussion in the report rather than silently discarded.
CREATE EXTERNAL TABLE IF NOT EXISTS nl_flagged_solar_gaps (
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
LOCATION '/big_data/distributed_energy/processed/nl_flagged_solar_gaps';

-- Sanity check row counts match what Pig reported
SELECT 'nl_readings' AS table_name, COUNT(*) AS row_count FROM nl_readings
UNION ALL
SELECT 'nl_flagged_solar_gaps' AS table_name, COUNT(*) AS row_count FROM nl_flagged_solar_gaps;
