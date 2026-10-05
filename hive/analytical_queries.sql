-- analytical_queries.sql
-- Descriptive / historical analytic queries.
-- Run after schema.sql has created the tables.

USE energy_db;

-- Q1: Peak demand by hour of day (averaged across the whole 3-month window)
-- Answers: "When are peak-demand periods?"
SELECT hour, ROUND(AVG(demand_mw), 1) AS avg_demand_mw, ROUND(MAX(demand_mw), 1) AS max_demand_mw
FROM readings
GROUP BY hour
ORDER BY avg_demand_mw DESC;

-- Q2: Renewable contribution over time, by month
-- Answers: "How much renewable generation is available?" and "What is the renewable share over time?"
SELECT MONTH(CAST(raw_timestamp AS TIMESTAMP)) AS month,
       ROUND(AVG(renewable_generation_mw), 1) AS avg_renewable_mw,
       ROUND(AVG(renewable_share), 3) AS avg_renewable_share
FROM readings
GROUP BY MONTH(CAST(raw_timestamp AS TIMESTAMP))
ORDER BY month;

-- Q3: Average net load by hour of day
-- Answers: "How does net load vary by hour/day/season/region?" (hour slice; full season breakdown needs more months of data than the current pilot)
SELECT hour, ROUND(AVG(net_load_mw), 1) AS avg_net_load_mw
FROM readings
GROUP BY hour
ORDER BY hour;

-- Q4: High-stress periods, top 20 by the project-defined grid_stress_indicator
-- Answers: "Which periods have high demand and low renewable generation?"
SELECT raw_timestamp, demand_mw, renewable_generation_mw, grid_stress_indicator
FROM readings
ORDER BY grid_stress_indicator DESC
LIMIT 20;

-- Q5: Demand vs temperature relationship, bucketed by 5-degree bands
-- Answers: "How does demand vary with weather?"
SELECT FLOOR(temperature_c / 5) * 5 AS temp_band_c,
       ROUND(AVG(demand_mw), 1) AS avg_demand_mw,
       COUNT(*) AS n_readings
FROM readings
GROUP BY FLOOR(temperature_c / 5) * 5
ORDER BY temp_band_c;

-- Q6: Daily demand and renewable share trend (one row per calendar day)
-- Supports the "daily/weekly/monthly demand patterns" question and doubles as a validation check that Pig's cleaning/integration worked correctly.
SELECT TO_DATE(CAST(raw_timestamp AS TIMESTAMP)) AS day,
       ROUND(AVG(demand_mw), 1) AS avg_demand_mw,
       ROUND(AVG(renewable_share), 3) AS avg_renewable_share,
       ROUND(AVG(grid_stress_indicator), 3) AS avg_grid_stress
FROM readings
GROUP BY TO_DATE(CAST(raw_timestamp AS TIMESTAMP))
ORDER BY day;

-- Q7: How many genuine solar data gaps per month 
SELECT MONTH(CAST(raw_timestamp AS TIMESTAMP)) AS month, COUNT(*) AS flagged_gap_count
FROM flagged_solar_gaps
GROUP BY MONTH(CAST(raw_timestamp AS TIMESTAMP))
ORDER BY month;
