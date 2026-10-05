# Energy Big Data Project

## WorkFlow

Git clone the repository and run the following steps in order:

#### 1. Fetch raw data from ENTSO-E Transparency Platform (XML) and convert to CSV.
```bash
cd Distributed-Renewable-Energy-Stress-Analysis
pip install -r scripts/requirements.txt
python scripts/fetch_entsoe.py --country NL --months 3
python scripts/fetch_weather.py --country NL --energy-csv data/processed/entsoe_NL_2026-07-06_2026-10-04.csv
```

#### 2. Clean and integrate the data using Pig.

- Start your Hadoop cluster and confirm hadoop is running.
- Create HDFS directory structure

```bash
hdfs dfs -mkdir -p /user/$USER/energy/raw
hdfs dfs -mkdir -p /user/$USER/energy/processed
```

- Upload the merged energy+weather CSV to HDFS
 
```bash
hdfs dfs -put data/processed/entsoe_weather_NL_2026-07-06_2026-10-04.csv /user/$USER/energy/raw/entsoe_weather_NL.csv
```

- Run the Pig ETL script

```bash
pig -x mapreduce -param USER=$USER pig/clean_integrate.pig
```

- Verify output

```bash
hdfs dfs -ls /user/$USER/energy/processed/nl_clean_integrated
hdfs dfs -cat /user/$USER/energy/processed/nl_clean_integrated/part-* | head -5

hdfs dfs -ls /user/$USER/energy/processed/nl_flagged_solar_gaps
hdfs dfs -cat /user/$USER/energy/processed/nl_flagged_solar_gaps/part-* | wc -l
```

#### 3. Analyze the data using Hive.

Yet to be implemented.

#### 4. Perform machine learning on the data using Spark.

Yet to be implemented.

## Project structure

```
data/raw/           raw XML responses (optional, for debugging)
data/processed/     cleaned CSV output
pig/                Pig ETL scripts (Phase 2)
hive/               Hive schema and queries (Phase 3)
spark/              Spark + MLlib code (Phase 4)
notebooks/          exploratory analysis
scripts/            data fetching and utility scripts
```
