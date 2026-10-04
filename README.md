# Energy Big Data Project — Phase 1: Data Pilot

## What this step does

Pulls 3 months of **Netherlands (NL)** data from the ENTSO-E Transparency Platform:
- Actual Load (demand)
- Solar generation
- Wind onshore generation
- Wind offshore generation

All four series come from the same source, same zone, same timestamp grid, so no cross-source join needed at this stage.

Goal: confirm data quality and volume before committing to the full dataset and before scaling to more countries.

## Setup (run on your own machine, not in a sandboxed environment)

```bash
cd energy-bigdata-project/scripts
pip install -r requirements.txt
cp ../.env.example ../.env
# edit ../.env and paste your real ENTSO-E token in place of "your_token_here"
```

## Run

```bash
python fetch_entsoe.py --country NL --months 3
```

Output:
- Processed CSV in `../data/processed/`
- Console summary: row count, date range, missing values per column, expected vs actual row count

## What to check in the output

1. **Missing values per column** — some gaps are expected (ENTSO-E has known data quality issues), but if solar/wind columns are mostly empty, NL may not be the right pilot country.
2. **Expected vs actual row count** — at 15-min resolution, 3 months is about 8,640 rows. A big shortfall means data isn't continuous.
3. **Date range** — confirm it actually covers the last 3 months.

## Next steps after this pilot

- If NL data quality looks solid: scale to the full BE/FR/NL/DE-LU bundle
- If not: try a different country or inspect gaps column by column
- Once satisfied: this becomes the HDFS ingestion input for Phase 2 (Pig ETL)

## Project structure

```
data/raw/          raw XML responses (optional, for debugging)
data/processed/    cleaned CSV output
pig/                Pig ETL scripts (Phase 2)
hive/               Hive schema and queries (Phase 3)
spark/              Spark + MLlib code (Phase 4)
notebooks/          exploratory analysis
docs/               proposal, report drafts
scripts/            data fetching and utility scripts
```
