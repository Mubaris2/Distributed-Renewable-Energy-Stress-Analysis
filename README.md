# Energy Big Data Project

## WorkFlow

Git clone the repository and run the following steps in order:

#### 1. Fetch raw data from ENTSO-E Transparency Platform (XML) and convert to CSV.
```bash
cd Distributed-Renewable-Energy-Stress-Analysis
# Start a virtual environment (optional but recommended)
pip install -r requirements.txt
python scripts/fetch_entsoe.py --country NL --months 3 # Multiple country support with comma-separated list, e.g., "NL,DE,FR" 
python scripts/fetch_weather.py --country NL 
```

#### 2. Clean and integrate the data using Pig.

- Start your Hadoop Cluster. Confirm that you can run `hdfs dfs -ls /` without any errors.
- Use `jps` to confirm that NameNode, DataNode, ResourceManager, and NodeManager are running.
- Create HDFS directory structure

```bash
hdfs dfs -mkdir -p /big_data/distributed_energy/raw
hdfs dfs -mkdir -p /big_data/distributed_energy/processed
```

- Upload the merged energy+weather CSV to HDFS
 
```bash
hdfs dfs -put data/processed/entsoe_weather_NL_2026-07-06_2026-10-04.csv /big_data/distributed_energy/raw/entsoe_weather_NL.csv
```

- Run the Pig ETL script

```bash
./scripts/run_pig_etl.sh --countries NL
```

- Verify output

```bash
hdfs dfs -ls /big_data/distributed_energy/processed/clean_integrated
hdfs dfs -cat /big_data/distributed_energy/processed/clean_integrated/part-* | head -5

hdfs dfs -ls /big_data/distributed_energy/processed/flagged_solar_gaps
hdfs dfs -cat /big_data/distributed_energy/processed/flagged_solar_gaps/part-* | wc -l
```

#### 3. Analyze the data using Hive.

- Start Hive server2 and connect to it using Beeline or Hive CLI.
- Inside `hive` shell, run the following commands to create the schema and run analytical queries:

```bash
SOURCE hive/schema.sql;
SOURCE hive/analytical_queries.sql;
```

Verify

```bash
hdfs dfs -ls /big_data/distributed_energy/processed/
```

#### 4. Perform machine learning on the data using Spark.

- Install and ensure the working of spark-submit and pyspark.
- Run the Spark MLlib code to train and compare models:

```bash
spark-submit spark/train_and_compare.py --target net_load_mw --models all --test-frac 0.2
spark-submit spark/train_and_compare.py --target demand_mw --models linear,rf --test-frac 0.2
```

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
