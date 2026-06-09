"""
Spark ETL job — sbi_raw.transactions → apply fraud detection rules → sbi_curated.*

Pipeline:
    1. Read the target date partition from sbi_raw.transactions
    2. Apply 3 rules from rules.py (HIGH_AMOUNT, VELOCITY, GEO_ANOMALY)
    3. Write results to 3 Iceberg tables:
       - sbi_curated.transactions   : all transactions with fraud flag
       - sbi_curated.fraud_alerts   : fraud transaction details
       - sbi_curated.fraud_summary  : aggregated by hour and channel

Usage:
    source config/env.conf
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --principal "${PRINCIPAL}" \
      --keytab "${KEYTAB}" \
      --properties-file /path/to/conf/spark_iceberg.conf \
      --py-files spark/etl/rules.py \
      spark/etl/fraud_detection_etl.py --dt 2024-06-15

# Air-gapped environment: --packages not available. Specify local JARs via spark.jars in conf/spark_iceberg.conf.
# Validated on: Python 3.9.21 / OpenJDK 11 / RHEL 9.6 / Cloudera CDP 7.3.1 (Spark 3.5 / Iceberg 1.5.2)
"""

import argparse
import logging
import os
import uuid
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Import rules module from the same package
import sys
sys.path.insert(0, os.path.dirname(__file__))
from rules import apply_all_rules


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("SBI-FraudETL")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KEYTAB    = os.environ.get("SPARK_KEYTAB",    os.environ.get("KEYTAB",    ""))
PRINCIPAL = os.environ.get("SPARK_PRINCIPAL", os.environ.get("PRINCIPAL", ""))

RAW_TABLE        = "sbi_raw.transactions"
CURATED_TRANS    = "sbi_curated.transactions"
CURATED_ALERTS   = "sbi_curated.fraud_alerts"
CURATED_SUMMARY  = "sbi_curated.fraud_summary"


# ---------------------------------------------------------------------------
# SparkSession creation
# ---------------------------------------------------------------------------

def build_spark_session(app_name: str = "SBI-FraudDetectionETL") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.kerberos.keytab",    KEYTAB)
        .config("spark.kerberos.principal", PRINCIPAL)
        # Iceberg optimizations
        .config("spark.sql.iceberg.merge-on-read.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Read raw data
# ---------------------------------------------------------------------------

def read_raw(spark: SparkSession, dt: str) -> DataFrame:
    """Reads raw transaction data for the specified date."""
    logger.info(f"Reading raw data: {RAW_TABLE}, dt={dt}")
    df = (
        spark.table(RAW_TABLE)
        .filter(F.col("dt") == dt)
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
    )
    count = df.count()
    logger.info(f"Raw record count: {count}")
    return df


# ---------------------------------------------------------------------------
# Write to curated tables
# ---------------------------------------------------------------------------

def write_curated_transactions(df: DataFrame, dt: str) -> None:
    """Writes all transactions with fraud flags to sbi_curated.transactions."""
    logger.info(f"Writing to sbi_curated.transactions (dt={dt})")

    curated = df.select(
        "transaction_id",
        "account_id",
        "event_timestamp",
        "amount",
        "merchant_id",
        "merchant_cat",
        "location_lat",
        "location_lon",
        "is_fraud",
        "fraud_reasons",
        F.current_timestamp().alias("etl_processed_at"),
        "dt",
        "channel",
    )

    (
        curated.writeTo(CURATED_TRANS)
        .option("mergeSchema", "true")
        .overwritePartitions()
    )
    logger.info(f"Write to sbi_curated.transactions complete")


def write_fraud_alerts(df: DataFrame, dt: str) -> None:
    """Writes each fraud transaction per fraud_reason to sbi_curated.fraud_alerts."""
    logger.info(f"Writing to sbi_curated.fraud_alerts (dt={dt})")

    gen_alert_id = F.udf(lambda: str(uuid.uuid4()), StringType())

    fraud_df = df.filter(F.col("is_fraud") == True)

    # Explode fraud_reasons into individual rows (handles multi-rule matches)
    alerts = (
        fraud_df
        .withColumn("fraud_reason_arr", F.split("fraud_reasons", ","))
        .withColumn("fraud_reason", F.explode("fraud_reason_arr"))
        .filter(F.col("fraud_reason") != "")
        .select(
            gen_alert_id().alias("alert_id"),
            "transaction_id",
            "account_id",
            "event_timestamp",
            "amount",
            "channel",
            "fraud_reason",
            "fraud_score",
            "location_lat",
            "location_lon",
            F.current_timestamp().alias("alerted_at"),
            "dt",
        )
    )

    alert_count = alerts.count()
    logger.info(f"Fraud alert count: {alert_count}")

    (
        alerts.writeTo(CURATED_ALERTS)
        .option("mergeSchema", "true")
        .overwritePartitions()
    )
    logger.info(f"Write to sbi_curated.fraud_alerts complete")


def write_fraud_summary(df: DataFrame, dt: str) -> None:
    """Writes fraud aggregates by hour and channel to sbi_curated.fraud_summary."""
    logger.info(f"Writing to sbi_curated.fraud_summary (dt={dt})")

    summary = (
        df
        .withColumn("hour", F.hour("event_timestamp"))
        .groupBy("dt", "hour", "channel")
        .agg(
            F.count("transaction_id").alias("total_txn"),
            F.sum(F.col("is_fraud").cast("long")).alias("fraud_txn"),
            F.sum(
                F.when(F.col("is_fraud"), F.col("amount")).otherwise(F.lit(0.0))
            ).alias("fraud_amount"),
        )
        .withColumn(
            "fraud_rate",
            F.round(
                F.when(
                    F.col("total_txn") > 0,
                    F.col("fraud_txn").cast("double") / F.col("total_txn") * 100
                ).otherwise(F.lit(0.0)),
                4
            )
        )
        .withColumn("summarized_at", F.current_timestamp())
    )

    (
        summary.writeTo(CURATED_SUMMARY)
        .option("mergeSchema", "true")
        .overwritePartitions()
    )
    logger.info(f"Write to sbi_curated.fraud_summary complete")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_etl(dt: str) -> None:
    logger.info(f"===== SBI Fraud Detection ETL starting: dt={dt} =====")
    spark = build_spark_session()

    try:
        raw_df = read_raw(spark, dt)

        if raw_df.rdd.isEmpty():
            logger.warning(f"No data to process for dt={dt}. Exiting ETL.")
            return

        logger.info("Applying fraud detection rules...")
        scored_df = apply_all_rules(raw_df)
        scored_df.cache()

        fraud_count = scored_df.filter(F.col("is_fraud")).count()
        total_count = scored_df.count()
        logger.info(f"Detection results: {fraud_count}/{total_count} fraud ({100*fraud_count/total_count:.2f}%)")

        write_curated_transactions(scored_df, dt)
        write_fraud_alerts(scored_df, dt)
        write_fraud_summary(scored_df, dt)

        scored_df.unpersist()

    finally:
        spark.stop()
        logger.info("===== SBI Fraud Detection ETL complete =====")


def main() -> None:
    parser = argparse.ArgumentParser(description="SBI Fraud Detection ETL")
    parser.add_argument(
        "--dt",
        type=str,
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Target processing date (YYYY-MM-DD, default: yesterday)",
    )
    args = parser.parse_args()
    run_etl(args.dt)


if __name__ == "__main__":
    main()
