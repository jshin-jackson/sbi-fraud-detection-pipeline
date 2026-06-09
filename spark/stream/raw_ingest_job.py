"""
Spark Batch Job — Kafka raw events → sbi_raw.transactions (Iceberg)
Intended to be scheduled via cron every minute.

Usage:
    source config/env.conf
    spark-submit \
      --master yarn \
      --deploy-mode client \
      --principal "${PRINCIPAL}" \
      --keytab "${KEYTAB}" \
      --properties-file conf/spark_iceberg.conf \
      spark/stream/raw_ingest_job.py

cron schedule (every minute):
    * * * * * /root/sbi-fraud-detection-pipeline/scripts/02_run_ingest.sh >> /var/log/sbi-ingest.log 2>&1

Environment variables:
    KAFKA_BROKERS     Kafka broker addresses
    KAFKA_TOPIC       Target topic (default: sbi-fd-transactions-raw)
    KAFKA_KEYTAB      Kerberos keytab path
    KAFKA_PRINCIPAL   Kerberos principal
    TRUSTSTORE_JKS    SSL truststore JKS path (TRUSTSTORE_JKS from env.conf)
    OFFSET_FILE       Kafka offset storage file path (default: /root/sbi-kafka-offsets.json)

# Air-gapped environment: --packages not available. Specify local JARs via spark.jars in conf/spark_iceberg.conf.
# Validated on: Python 3.9.21 / OpenJDK 11 / RHEL 9.6 / Cloudera CDP 7.3.1 (Spark 3.5 / Iceberg 1.5.2)
"""

import json
import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp,
    max as spark_max, to_timestamp, when
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("SBI-RawIngest")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKERS      = os.environ.get("KAFKA_BROKERS",      "")
KAFKA_TOPIC        = os.environ.get("KAFKA_TOPIC",        "sbi-fd-transactions-raw")
KAFKA_KEYTAB       = os.environ.get("KAFKA_KEYTAB",       os.environ.get("KEYTAB",    ""))
KAFKA_PRINCIPAL    = os.environ.get("KAFKA_PRINCIPAL",    os.environ.get("PRINCIPAL", ""))
KAFKA_TRUSTSTORE   = os.environ.get("KAFKA_TRUSTSTORE",   os.environ.get("TRUSTSTORE_JKS", ""))
KAFKA_TRUSTSTORE_PW = os.environ.get("TRUSTSTORE_PW",    "")

ICEBERG_TABLE = "sbi_raw.transactions"
OFFSET_FILE   = os.environ.get("OFFSET_FILE", "/root/sbi-kafka-offsets.json")


# ---------------------------------------------------------------------------
# Kafka event JSON schema
# ---------------------------------------------------------------------------
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  nullable=False),
    StructField("account_id",     StringType(),  nullable=False),
    StructField("timestamp",      StringType(),  nullable=True),
    StructField("amount",         DoubleType(),  nullable=True),
    StructField("merchant_id",    StringType(),  nullable=True),
    StructField("merchant_cat",   StringType(),  nullable=True),
    StructField("location_lat",   DoubleType(),  nullable=True),
    StructField("location_lon",   DoubleType(),  nullable=True),
    StructField("channel",        StringType(),  nullable=True),
    StructField("is_fraud",       BooleanType(), nullable=True),
])


def load_starting_offsets() -> str:
    """
    Reads the saved offset file and returns the startingOffsets JSON string.
    Returns "earliest" if the file does not exist.
    """
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE) as f:
            offsets = json.load(f)
        logger.info(f"Offset file loaded: {offsets}")
        return json.dumps(offsets)
    logger.info("No offset file found — starting from earliest")
    return "earliest"


def save_offsets(raw_df) -> None:
    """Saves the maximum offset + 1 from the processed batch to file."""
    rows = (
        raw_df.groupBy("partition")
        .agg(spark_max("offset").alias("offset"))
        .collect()
    )
    offsets = {KAFKA_TOPIC: {str(row["partition"]): row["offset"] + 1 for row in rows}}
    with open(OFFSET_FILE, "w") as f:
        json.dump(offsets, f)
    logger.info(f"Offsets saved: {offsets}")


def build_spark_session() -> SparkSession:
    """Creates a SparkSession with Kerberos and Iceberg configuration."""
    spark = (
        SparkSession.builder
        .appName("SBI-RawTransactionIngest-Batch")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.kerberos.keytab", KAFKA_KEYTAB)
        .config("spark.kerberos.principal", KAFKA_PRINCIPAL)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_batch(spark: SparkSession, starting_offsets: str):
    """Reads a batch DataFrame from the Kafka topic."""
    # Pass JAAS config as an inline string — no file distribution needed, works on all executors
    jaas_conf = (
        "com.sun.security.auth.module.Krb5LoginModule required "
        "useKeyTab=true "
        f"keyTab=\"{KAFKA_KEYTAB}\" "
        f"principal=\"{KAFKA_PRINCIPAL}\";"
    )
    return (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol",          "SASL_SSL")
        .option("kafka.sasl.mechanism",             "GSSAPI")
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("kafka.sasl.jaas.config",           jaas_conf)
        .option("kafka.ssl.truststore.location",    KAFKA_TRUSTSTORE)
        .option("kafka.ssl.truststore.password",    KAFKA_TRUSTSTORE_PW)
        .load()
    )


def parse_and_enrich(raw_df):
    """Parses raw Kafka bytes and adds metadata columns."""
    return (
        raw_df
        .select(
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
        )
        .select(
            col("kafka_offset"),
            col("kafka_partition"),
            col("data.transaction_id"),
            col("data.account_id"),
            to_timestamp(col("data.timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
                .alias("event_timestamp"),
            col("data.amount"),
            col("data.merchant_id"),
            col("data.merchant_cat"),
            col("data.location_lat"),
            col("data.location_lon"),
            col("data.channel"),
            col("data.is_fraud").alias("is_fraud_label"),
            current_timestamp().alias("ingested_at"),
        )
        .withColumn(
            "dt",
            when(
                col("event_timestamp").isNotNull(),
                col("event_timestamp").cast("date").cast("string")
            ).otherwise(
                current_timestamp().cast("date").cast("string")
            )
        )
        .filter(col("transaction_id").isNotNull())
    )


def main() -> None:
    logger.info("SBI Raw Transaction Ingest Batch job starting")

    starting_offsets = load_starting_offsets()
    spark = build_spark_session()
    logger.info(f"Spark version: {spark.version}")

    raw_df = read_kafka_batch(spark, starting_offsets)

    total = raw_df.count()
    if total == 0:
        logger.info("No new messages, exiting")
        spark.stop()
        sys.exit(0)

    logger.info(f"Read {total} records from Kafka")

    # Save offsets (prevents duplicate processing on next run)
    save_offsets(raw_df)

    enriched = parse_and_enrich(raw_df)
    write_count = enriched.count()

    logger.info(f"Writing {write_count} records to Iceberg → {ICEBERG_TABLE}")
    (
        enriched.writeTo(ICEBERG_TABLE)
        .option("mergeSchema", "true")
        .append()
    )
    logger.info(f"Write complete: {write_count} records → {ICEBERG_TABLE}")

    spark.stop()
    logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
