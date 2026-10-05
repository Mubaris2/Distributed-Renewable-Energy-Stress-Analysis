"""
Phase 4: Spark MLlib model training and comparison.

Reads the cleaned/integrated energy data, engineers forecasting features,
trains multiple candidate models with cross-validated hyperparameter search,
evaluates each on a chronological (per-country) held-out test set, and writes
a comparison table plus the best model's predictions.

Usage (spark-submit):
    spark-submit train_and_compare.py --target net_load_mw --models all --test-frac 0.2
    spark-submit train_and_compare.py --target demand_mw --models linear,rf --test-frac 0.2

See docs/phase4_spark_mllib_plan.md for the full design rationale,
particularly around why the train/test split must be chronological and why
lag features are partitioned by country.
"""

import argparse
import time
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

MODEL_REGISTRY = {
    "linear": "Linear Regression",
    "dt": "Decision Tree",
    "rf": "Random Forest",
    "gbt": "Gradient-Boosted Trees",
}

LAG_STEPS = {
    "previous_hour_demand": 4,     # 4 * 15min = 1 hour
    "previous_day_demand": 96,     # 96 * 15min = 1 day
    "previous_week_demand": 672,   # 672 * 15min = 1 week
}


def build_spark():
    return (
        SparkSession.builder
        .appName("energy-forecast-model-comparison")
        .enableHiveSupport()
        .getOrCreate()
    )

def load_data(spark, use_hive=True, hdfs_path="/big_data/distributed_energy/processed/clean_integrated"):
    if use_hive:
        df = spark.sql("SELECT * FROM energy_db.readings")
    else:
        schema_cols = [
            "country", "raw_timestamp", "demand_mw", "solar_mw", "wind_onshore_mw",
            "wind_offshore_mw", "temperature_c", "humidity_pct", "wind_speed_ms", "hour",
            "renewable_generation_mw", "net_load_mw", "renewable_share", "grid_stress_indicator",
        ]
        df = spark.read.csv(hdfs_path, header=False, inferSchema=True).toDF(*schema_cols)
    return df.withColumn("ts", F.to_timestamp("raw_timestamp", "yyyy-MM-dd HH:mm:ss"))


def engineer_features(df):
    df = df.withColumn("day_of_week", F.dayofweek("ts")) \
           .withColumn("month", F.month("ts")) \
           .withColumn("season", F.when(F.col("month").isin(12, 1, 2), "winter")
                                   .when(F.col("month").isin(3, 4, 5), "spring")
                                   .when(F.col("month").isin(6, 7, 8), "summer")
                                   .otherwise("autumn"))

    # Lag features, windowed PER COUNTRY, ordered by time. Partitioning by
    # country is essential, without it a lag could pull in another
    # country's demand value.
    w = Window.partitionBy("country").orderBy("ts")
    for col_name, steps in LAG_STEPS.items():
        df = df.withColumn(col_name, F.lag("demand_mw", steps).over(w))

    before = df.count()
    df = df.na.drop(subset=list(LAG_STEPS.keys()))
    after = df.count()
    print(f"Dropped {before - after} rows with no lag history yet (expected at the start of each country's series).")
    return df


def chronological_split(df, test_frac):
    """Per-country chronological split: first (1-test_frac) of each
    country's timeline is train, the rest is test. Never randomSplit here."""
    w = Window.partitionBy("country").orderBy("ts")
    df = df.withColumn("row_num", F.row_number().over(w))
    counts = df.groupBy("country").agg(F.count("*").alias("n")).collect()
    country_cutoffs = {row["country"]: int(row["n"] * (1 - test_frac)) for row in counts}

    train_frames, test_frames = [], []
    for country, cutoff in country_cutoffs.items():
        country_df = df.filter(F.col("country") == country)
        train_frames.append(country_df.filter(F.col("row_num") <= cutoff))
        test_frames.append(country_df.filter(F.col("row_num") > cutoff))

    train_df = train_frames[0]
    for f in train_frames[1:]:
        train_df = train_df.union(f)
    test_df = test_frames[0]
    for f in test_frames[1:]:
        test_df = test_df.union(f)

    return train_df.drop("row_num"), test_df.drop("row_num")


def build_feature_pipeline(df, multi_country):
    feature_cols = [
        "hour", "day_of_week", "month",
        "temperature_c", "humidity_pct", "wind_speed_ms",
        "solar_mw", "wind_onshore_mw", "wind_offshore_mw",
        "previous_hour_demand", "previous_day_demand", "previous_week_demand",
    ]
    stages = []

    # One-hot encode season always; one-hot encode country only if this run
    # actually has more than one country (pooled-model case).
    season_indexer = StringIndexer(inputCol="season", outputCol="season_idx", handleInvalid="keep")
    season_ohe = OneHotEncoder(inputCols=["season_idx"], outputCols=["season_vec"])
    stages += [season_indexer, season_ohe]
    feature_cols.append("season_vec")

    if multi_country:
        country_indexer = StringIndexer(inputCol="country", outputCol="country_idx", handleInvalid="keep")
        country_ohe = OneHotEncoder(inputCols=["country_idx"], outputCols=["country_vec"])
        stages += [country_indexer, country_ohe]
        feature_cols.append("country_vec")

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    stages.append(assembler)
    return stages


def get_model_and_grid(model_key, target_col):
    if model_key == "linear":
        model = LinearRegression(featuresCol="features", labelCol=target_col)
        grid = (ParamGridBuilder()
                .addGrid(model.regParam, [0.0, 0.01, 0.1, 1.0])
                .addGrid(model.elasticNetParam, [0.0, 0.5, 1.0])
                .build())
    elif model_key == "dt":
        model = DecisionTreeRegressor(featuresCol="features", labelCol=target_col)
        grid = (ParamGridBuilder()
                .addGrid(model.maxDepth, [5, 10, 15, 20])
                .addGrid(model.maxBins, [32, 64])
                .build())
    elif model_key == "rf":
        model = RandomForestRegressor(featuresCol="features", labelCol=target_col)
        grid = (ParamGridBuilder()
                .addGrid(model.numTrees, [20, 50, 100])
                .addGrid(model.maxDepth, [5, 10, 15])
                .build())
    elif model_key == "gbt":
        model = GBTRegressor(featuresCol="features", labelCol=target_col)
        grid = (ParamGridBuilder()
                .addGrid(model.maxIter, [20, 50, 100])
                .addGrid(model.maxDepth, [3, 5, 8])
                .addGrid(model.stepSize, [0.05, 0.1, 0.2])
                .build())
    else:
        raise ValueError(f"Unknown model key: {model_key}")
    return model, grid


def evaluate(predictions, target_col):
    results = {}
    for metric in ["mae", "rmse", "r2"]:
        evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName=metric)
        results[metric] = evaluator.evaluate(predictions)
    # MAPE, manual since Spark's RegressionEvaluator doesn't provide it directly
    mape_df = predictions.withColumn(
        "ape", F.abs((F.col(target_col) - F.col("prediction")) / F.col(target_col))
    )
    results["mape"] = mape_df.agg(F.avg("ape")).first()[0] * 100
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="net_load_mw", choices=["net_load_mw", "demand_mw"])
    parser.add_argument("--models", default="all", help="Comma-separated: linear,dt,rf,gbt or 'all'")
    parser.add_argument("--test-frac", type=float, default=0.2)
    parser.add_argument("--use-hive", action="store_true", default=True)
    parser.add_argument("--output-dir", default=str(Path(__file__).resolve().parent.parent / "notebooks"),
                         help="Where to save model_comparison.csv. Defaults to the project's "
                              "notebooks/ folder, resolved relative to this script's own location "
                              "so it lands in the right place regardless of the shell's cwd when "
                              "spark-submit is called. Pass an absolute path to override.")
    args = parser.parse_args()

    model_keys = list(MODEL_REGISTRY.keys()) if args.models == "all" else [m.strip() for m in args.models.split(",")]

    spark = build_spark()
    df = load_data(spark, use_hive=args.use_hive)
    df = engineer_features(df)

    multi_country = df.select("country").distinct().count() > 1
    print(f"Countries in this run: {[r['country'] for r in df.select('country').distinct().collect()]}")
    print(f"Pooled model across countries: {multi_country}")

    train_df, test_df = chronological_split(df, args.test_frac)
    print(f"Train rows: {train_df.count()}, Test rows: {test_df.count()}")

    feature_stages = build_feature_pipeline(df, multi_country)

    comparison_rows = []
    best_model_name, best_rmse, best_predictions = None, float("inf"), None

    for model_key in model_keys:
        model_name = MODEL_REGISTRY[model_key]
        print(f"\n=== Training {model_name} ===")
        model, grid = get_model_and_grid(model_key, args.target)
        pipeline = Pipeline(stages=feature_stages + [model])
        evaluator = RegressionEvaluator(labelCol=args.target, predictionCol="prediction", metricName="rmse")
        cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3, parallelism=2)

        start = time.time()
        cv_model = cv.fit(train_df)
        train_time = time.time() - start

        predictions = cv_model.transform(test_df)
        metrics = evaluate(predictions, args.target)
        best_params = cv_model.getEstimatorParamMaps()[cv_model.avgMetrics.index(min(cv_model.avgMetrics))]
        best_params_str = ", ".join(f"{p.name}={v}" for p, v in best_params.items())

        print(f"{model_name}: MAE={metrics['mae']:.2f}, RMSE={metrics['rmse']:.2f}, "
              f"R2={metrics['r2']:.3f}, MAPE={metrics['mape']:.2f}%, train_time={train_time:.1f}s")
        print(f"Best params: {best_params_str}")

        comparison_rows.append({
            "model": model_name,
            "best_params": best_params_str,
            "mae": metrics["mae"],
            "rmse": metrics["rmse"],
            "r2": metrics["r2"],
            "mape_pct": metrics["mape"],
            "train_time_sec": round(train_time, 1),
        })

        if metrics["rmse"] < best_rmse:
            best_rmse = metrics["rmse"]
            best_model_name = model_name
            best_predictions = predictions

    # ===== Comparison table =====
    comparison_df = spark.createDataFrame(comparison_rows).orderBy("rmse")
    print("\n=== Model comparison (sorted by RMSE, best first) ===")
    comparison_df.show(truncate=False)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "model_comparison.csv"
    comparison_df.toPandas().to_csv(output_path, index=False)
    print(f"Saved {output_path}, this is the table for the report.")
    print(f"\nBest model: {best_model_name} (RMSE={best_rmse:.2f})")

    # ===== Write best model's predictions back to Hive =====
    forecast_output = best_predictions.select(
        "country", "raw_timestamp",
        F.col(args.target).alias("actual"),
        F.col("prediction").alias("predicted"),
        F.lit(best_model_name).alias("model_name"),
    )
    forecast_output.write.mode("overwrite").saveAsTable("energy_db.forecast_results")
    print("Wrote best model predictions to energy_db.forecast_results")

    spark.stop()


if __name__ == "__main__":
    main()
