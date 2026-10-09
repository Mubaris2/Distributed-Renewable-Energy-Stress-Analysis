#!/usr/bin/env python3
"""Terminal-only Spark ML comparison for electricity net-load forecasting.

Reads energy_db.readings from Hive. Does not write CSV files or prediction
results to Hive. Example:
spark-submit --driver-memory 3g --conf spark.driver.maxResultSize=512m \
  --conf spark.sql.shuffle.partitions=8 spark/train_and_compare.py \
  --target net_load_mw --models all --test-frac 0.2 --skip-cv
"""
import argparse
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import (
    LinearRegression, DecisionTreeRegressor,
    RandomForestRegressor, GBTRegressor,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

DATABASE = "energy_db"
SOURCE_TABLE = "readings"
MODEL_NAMES = {
    "linear": "Linear Regression",
    "dt": "Decision Tree",
    "rf": "Random Forest",
    "gbt": "Gradient-Boosted Trees",
}


def parse_args():
    p = argparse.ArgumentParser(description="Compare Spark forecasting regressors.")
    p.add_argument("--target", default="net_load_mw")
    p.add_argument("--models", default="all",
                   help="all or comma-separated: linear,dt,rf,gbt")
    p.add_argument("--test-frac", type=float, default=0.2)
    p.add_argument("--folds", type=int, default=3)
    p.add_argument("--skip-cv", action="store_true")
    return p.parse_args()


def make_spark():
    return (SparkSession.builder
            .appName("DistributedEnergyStressModelComparison")
            .enableHiveSupport().getOrCreate())


def first_existing(columns, candidates):
    lookup = {c.lower(): c for c in columns}
    return next((lookup[x.lower()] for x in candidates
                 if x.lower() in lookup), None)


def parse_timestamp(df):
    if "event_timestamp" in df.columns:
        return df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    name = first_existing(df.columns, [
        "raw_timestamp", "timestamp", "datetime", "date_time"
    ])
    if name is None:
        raise ValueError("No event_timestamp or raw_timestamp column found.")
    raw = F.col(name).cast("string")
    parsed = F.coalesce(
        F.to_timestamp(raw),
        F.to_timestamp(raw, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(raw, "yyyy-MM-dd HH:mm"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(raw, "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(raw, "dd/MM/yyyy HH:mm"),
    )
    return df.withColumn("event_timestamp", parsed)


def prepare_data(spark, target, test_frac):
    table = f"{DATABASE}.{SOURCE_TABLE}"
    print(f"Reading Hive table: {table}", flush=True)
    df = spark.table(table)
    print(f"Source columns: {df.columns}", flush=True)
    if target not in df.columns:
        raise ValueError(f"Target {target!r} not found in {table}. Columns: {df.columns}")

    df = parse_timestamp(df).filter(F.col("event_timestamp").isNotNull())
    print(f"Rows with parsed timestamps: {df.count()}", flush=True)
    df = df.withColumn(target, F.col(target).cast("double"))
    df = df.filter(F.col(target).isNotNull() & ~F.isnan(F.col(target)))

    country_col = first_existing(df.columns, ["country", "country_name", "region"])
    if country_col is None:
        df = df.withColumn("country", F.lit("ALL"))
    elif country_col != "country":
        df = df.withColumnRenamed(country_col, "country")

    excluded = {
        target.lower(), "event_timestamp", "raw_timestamp", "timestamp",
        "datetime", "date_time", "time", "country", "country_name",
        "region", "id", "index", "grid_stress_indicator",
    }
    # In this dataset net_load_mw = demand_mw - renewable_generation_mw.
    # Exclude current-time energy measurements that reveal that target.
    if target.lower() == "net_load_mw":
        excluded.update({
            "demand_mw", "solar_mw", "wind_onshore_mw",
            "wind_offshore_mw", "renewable_generation_mw", "renewable_share",
        })

    covariates = [
        field.name for field in df.schema.fields
        if field.name.lower() not in excluded
        and isinstance(field.dataType, NumericType)
    ]
    for name in covariates:
        val = F.col(name).cast("double")
        df = df.withColumn(
            name,
            F.when(val.isNull() | F.isnan(val), F.lit(0.0)).otherwise(val)
        )

    # Derive calendar values from the parsed timestamp.
    df = (df.withColumn("hour", F.hour("event_timestamp").cast("double"))
          .withColumn("day_of_week",
                      (F.dayofweek("event_timestamp") - 1).cast("double"))
          .withColumn("month", F.month("event_timestamp").cast("double"))
          .withColumn(
              "season",
              F.when(F.month("event_timestamp").isin(12, 1, 2), "winter")
               .when(F.month("event_timestamp").isin(3, 4, 5), "spring")
               .when(F.month("event_timestamp").isin(6, 7, 8), "summer")
               .otherwise("autumn")))

    # Assumes 15-minute observations: 4/hour, 96/day, 672/week.
    w = Window.partitionBy("country").orderBy("event_timestamp")
    df = (df.withColumn("previous_hour_demand", F.lag(F.col(target), 4).over(w))
          .withColumn("previous_day_demand", F.lag(F.col(target), 96).over(w))
          .withColumn("previous_week_demand", F.lag(F.col(target), 672).over(w)))

    before = df.count()
    lag_cols = ["previous_hour_demand", "previous_day_demand", "previous_week_demand"]
    df = df.dropna(subset=lag_cols)
    after = df.count()
    print(f"Rows removed for insufficient lag history: {before-after}", flush=True)
    print(f"Rows available after feature preparation: {after}", flush=True)

    features = ["hour", "day_of_week", "month"] + lag_cols
    for name in covariates:
        if name not in features and name != target:
            features.append(name)
    df = df.dropna(subset=["event_timestamp", "country", target, "season"] + features)
    return df, target, features, test_frac


def model_and_grid(key, target):
    if key == "linear":
        model = LinearRegression(
            featuresCol="features", labelCol=target, predictionCol="prediction",
            maxIter=100, regParam=0.1, elasticNetParam=0.0, standardization=True)
        grid = (ParamGridBuilder().addGrid(model.regParam, [0.01, 0.1, 1.0])
                .addGrid(model.elasticNetParam, [0.0, 0.5, 1.0]).build())
    elif key == "dt":
        model = DecisionTreeRegressor(
            featuresCol="features", labelCol=target, predictionCol="prediction",
            maxBins=32)
        grid = ParamGridBuilder().addGrid(model.maxDepth, [5, 10, 15]).build()
    elif key == "rf":
        model = RandomForestRegressor(
            featuresCol="features", labelCol=target, predictionCol="prediction",
            numTrees=10, maxDepth=5, maxBins=16, maxMemoryInMB=128,
            subsamplingRate=0.8, featureSubsetStrategy="auto", seed=42)
        grid = ParamGridBuilder().build()
    elif key == "gbt":
        model = GBTRegressor(
            featuresCol="features", labelCol=target, predictionCol="prediction",
            maxIter=30, maxDepth=3, stepSize=0.1, maxBins=32, seed=42)
        grid = (ParamGridBuilder().addGrid(model.maxIter, [20, 30])
                .addGrid(model.maxDepth, [3, 5]).build())
    else:
        raise ValueError(f"Unknown model: {key}")
    return model, grid


def make_pipeline(model, features):
    indexer = StringIndexer(inputCol="season", outputCol="season_index",
                            handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["season_index"],
                            outputCols=["season_vector"], handleInvalid="keep")
    assembler = VectorAssembler(inputCols=features + ["season_vector"],
                                outputCol="features", handleInvalid="skip")
    return Pipeline(stages=[indexer, encoder, assembler, model])


def calculate_metrics(preds, target):
    def evaluate(metric):
        return RegressionEvaluator(
            labelCol=target, predictionCol="prediction",
            metricName=metric).evaluate(preds)
    mae, rmse, r2 = evaluate("mae"), evaluate("rmse"), evaluate("r2")
    mape = preds.select(F.avg(F.when(
        F.abs(F.col(target)) > 1e-9,
        F.abs(F.col(target) - F.col("prediction")) / F.abs(F.col(target))
    )).alias("mape")).first()["mape"]
    return float(mae), float(rmse), float(r2), float(mape or 0.0) * 100.0


def main():
    args = parse_args()
    if not 0.05 <= args.test_frac <= 0.5:
        raise ValueError("--test-frac must be between 0.05 and 0.5")
    if args.folds < 2:
        raise ValueError("--folds must be at least 2")
    requested = (["linear", "dt", "rf", "gbt"] if args.models.lower() == "all"
                 else [x.strip().lower() for x in args.models.split(",") if x.strip()])
    if not requested:
        raise ValueError("No models selected.")
    unknown = set(requested) - set(MODEL_NAMES)
    if unknown:
        raise ValueError(f"Unknown model(s): {sorted(unknown)}")

    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")
    prepared = None
    results = []
    try:
        df, target, features, _ = prepare_data(spark, args.target, args.test_frac)
        prepared = df.persist(StorageLevel.MEMORY_AND_DISK)
        print(f"Materialized prepared rows: {prepared.count()}", flush=True)
        countries = [r["country"] for r in prepared.select("country").distinct().collect()]
        print(f"Countries to process: {countries}", flush=True)
        print(f"Numeric features: {features}", flush=True)

        for country in countries:
            country_df = (prepared.filter(F.col("country") == country)
                          .orderBy("event_timestamp")
                          .persist(StorageLevel.MEMORY_AND_DISK))
            count = country_df.count()
            if count < 100:
                print(f"Skipping {country}: only {count} usable rows.", flush=True)
                country_df.unpersist()
                continue
            split = int(count * (1.0 - args.test_frac))
            ordered = country_df.withColumn(
                "__rn", F.row_number().over(Window.orderBy("event_timestamp")))
            train = (ordered.filter(F.col("__rn") <= split).drop("__rn")
                     .persist(StorageLevel.MEMORY_AND_DISK))
            test = (ordered.filter(F.col("__rn") > split).drop("__rn")
                    .persist(StorageLevel.MEMORY_AND_DISK))
            ntrain, ntest = train.count(), test.count()
            print(f"\nCountry={country}; rows={count}; train={ntrain}; test={ntest}",
                  flush=True)
            if not ntrain or not ntest:
                train.unpersist(); test.unpersist(); country_df.unpersist()
                continue

            for key in requested:
                print(f"\nTraining model: {key}", flush=True)
                start = time.time()
                model, grid = model_and_grid(key, target)
                pipeline = make_pipeline(model, features)
                if args.skip_cv or key == "rf":
                    fitted = pipeline.fit(train)
                else:
                    evaluator = RegressionEvaluator(
                        labelCol=target, predictionCol="prediction", metricName="rmse")
                    cv = CrossValidator(
                        estimator=pipeline, estimatorParamMaps=grid,
                        evaluator=evaluator, numFolds=args.folds,
                        parallelism=1, seed=42)
                    fitted = cv.fit(train).bestModel
                elapsed = time.time() - start

                preds = (fitted.transform(test)
                         .select("country", "event_timestamp",
                                 F.col(target).alias("actual"),
                                 F.col("prediction").cast("double").alias("prediction"))
                         .persist(StorageLevel.MEMORY_AND_DISK))
                pred_count = preds.count()
                if pred_count == 0:
                    print(f"{key}: no test predictions; skipping.", flush=True)
                    preds.unpersist()
                    continue
                metric_input = preds.withColumn(target, F.col("actual"))
                mae, rmse, r2, mape = calculate_metrics(metric_input, target)
                print(f"{key.upper()} | MAE={mae:.2f} | RMSE={rmse:.2f} | "
                      f"R2={r2:.3f} | MAPE={mape:.2f}% | train_time={elapsed:.1f}s",
                      flush=True)
                results.append({
                    "country": country, "model": MODEL_NAMES[key],
                    "mae": mae, "rmse": rmse, "r2": r2, "mape": mape,
                    "time": elapsed, "rows": pred_count,
                })
                preds.unpersist()
                del fitted
            train.unpersist(); test.unpersist(); country_df.unpersist()

        if not results:
            raise RuntimeError("No model produced results; check the data and logs.")
        print("\nFinal model comparison (sorted by RMSE):", flush=True)
        for r in sorted(results, key=lambda x: x["rmse"]):
            print(f"{r['country']} | {r['model']} | MAE={r['mae']:.2f} | "
                  f"RMSE={r['rmse']:.2f} | R2={r['r2']:.3f} | "
                  f"MAPE={r['mape']:.2f}% | train_time={r['time']:.1f}s",
                  flush=True)
    finally:
        if prepared is not None:
            prepared.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
