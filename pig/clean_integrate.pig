-- ===== 1. LOAD raw merged energy+weather CSV(s), skip header, cast types =====
raw = LOAD '$RAW_PATHS'
    USING PigStorage(',')
    AS (
        country:chararray,
        timestamp:chararray,
        load_mw:chararray,
        solar_mw:chararray,
        wind_onshore_mw:chararray,
        wind_offshore_mw:chararray,
        temperature_c:chararray,
        humidity_pct:chararray,
        wind_speed_ms:chararray
    );

-- drop the header row 
raw_no_header = FILTER raw BY country != 'country';

-- explicit type casting
casted = FOREACH raw_no_header GENERATE
    country AS country,
    timestamp AS timestamp,
    (load_mw == '' ? (double)null : (double)load_mw) AS load_mw:double,
    (solar_mw == '' ? (double)null : (double)solar_mw) AS solar_mw:double,
    (wind_onshore_mw == '' ? (double)null : (double)wind_onshore_mw) AS wind_onshore_mw:double,
    (wind_offshore_mw == '' ? (double)null : (double)wind_offshore_mw) AS wind_offshore_mw:double,
    (temperature_c == '' ? (double)null : (double)temperature_c) AS temperature_c:double,
    (humidity_pct == '' ? (double)null : (double)humidity_pct) AS humidity_pct:double,
    (wind_speed_ms == '' ? (double)null : (double)wind_speed_ms) AS wind_speed_ms:double;

-- ===== 2. Extract hour, needed both for solar imputation logic and as a feature =====
with_hour = FOREACH casted GENERATE *,
    GetHour(ToDate(timestamp, 'yyyy-MM-dd HH:mm:ss')) AS hour:int;

-- ===== 3. FILTER invalid physical values =====
physically_valid = FILTER with_hour BY
    load_mw IS NOT NULL AND load_mw > 0
    AND (wind_onshore_mw IS NULL OR wind_onshore_mw >= 0)
    AND (wind_offshore_mw IS NULL OR wind_offshore_mw >= 0)
    AND (solar_mw IS NULL OR solar_mw >= 0);

-- ===== 4. Missing-value handling for solar =====
solar_handled = FOREACH physically_valid GENERATE
    country, timestamp, load_mw,
    ((solar_mw IS NULL AND (hour >= 22 OR hour < 5)) ? 0.0 : solar_mw) AS solar_mw,
    wind_onshore_mw, wind_offshore_mw,
    temperature_c, humidity_pct, wind_speed_ms, hour;

clean_main = FILTER solar_handled BY solar_mw IS NOT NULL;
flagged_daylight_gaps = FILTER solar_handled BY solar_mw IS NULL;

-- ===== 5. Deduplication, keyed on (country, timestamp) =====
grouped_by_ts = GROUP clean_main BY (country, timestamp);
deduped = FOREACH grouped_by_ts GENERATE
    FLATTEN(TOP(1, 0, clean_main)) AS (
        country, timestamp, load_mw, solar_mw, wind_onshore_mw, wind_offshore_mw,
        temperature_c, humidity_pct, wind_speed_ms, hour
    );

-- ===== 6. Derived project-defined energy metrics =====
with_metrics = FOREACH deduped GENERATE
    country, timestamp, load_mw AS demand_mw, solar_mw, wind_onshore_mw, wind_offshore_mw,
    temperature_c, humidity_pct, wind_speed_ms, hour,
    (solar_mw + wind_onshore_mw + wind_offshore_mw) AS renewable_generation_mw,
    (load_mw - (solar_mw + wind_onshore_mw + wind_offshore_mw)) AS net_load_mw,
    ((solar_mw + wind_onshore_mw + wind_offshore_mw) / load_mw) AS renewable_share,
    (1.0 - ((solar_mw + wind_onshore_mw + wind_offshore_mw) / load_mw)) AS grid_stress_indicator;

-- ===== 7. STORE cleaned + flagged outputs to HDFS, ready for Hive external tables =====
STORE with_metrics INTO '/big_data/distributed_energy/processed/clean_integrated'
    USING PigStorage(',');
STORE flagged_daylight_gaps INTO '/big_data/distributed_energy/processed/flagged_solar_gaps'
    USING PigStorage(',');
