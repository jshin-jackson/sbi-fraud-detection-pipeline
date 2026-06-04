"""
Spark Structured Streaming 잡 — Kafka 원시 이벤트 → sbi_raw.transactions (Iceberg)

실행 방법:
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --principal sbi-spark@SBI.LOCAL \
      --keytab /etc/security/keytabs/sbi-spark.keytab \
      --files /etc/security/keytabs/sbi-spark.keytab,/path/to/kafka_kerberos.properties \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
      --conf spark.sql.catalog.spark_catalog.type=hive \
      --conf spark.sql.catalog.spark_catalog.uri=thrift://hiveserver2.sbi.local:9083 \
      --conf spark.hadoop.fs.s3a.endpoint=http://ozone-s3g.sbi.local:9878 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      raw_ingest_job.py
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    to_timestamp, when
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, LongType, IntegerType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("SBI-RawIngest")


# ---------------------------------------------------------------------------
# 환경 설정
# ---------------------------------------------------------------------------
KAFKA_BROKERS    = os.environ.get("KAFKA_BROKERS",    "kafka-broker1.sbi.local:9093")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC",      "sbi.transactions.raw")
KAFKA_GROUP_ID   = os.environ.get("KAFKA_GROUP_ID",   "sbi-spark-stream-group")
KAFKA_KEYTAB     = os.environ.get("KAFKA_KEYTAB",     "/etc/security/keytabs/sbi-spark.keytab")
KAFKA_PRINCIPAL  = os.environ.get("KAFKA_PRINCIPAL",  "sbi-spark@SBI.LOCAL")
KAFKA_TRUSTSTORE = os.environ.get("KAFKA_TRUSTSTORE", "/etc/security/certs/truststore.jks")
KAFKA_TRUSTSTORE_PW = os.environ.get("KAFKA_TRUSTSTORE_PW", "changeit")

ICEBERG_TABLE    = "sbi_raw.transactions"
CHECKPOINT_PATH  = os.environ.get(
    "CHECKPOINT_PATH",
    "s3a://sbi-raw/checkpoints/raw-ingest"
)
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
OUTPUT_MODE      = "append"


# ---------------------------------------------------------------------------
# Kafka 이벤트 JSON 스키마
# ---------------------------------------------------------------------------
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  nullable=False),
    StructField("account_id",     StringType(),  nullable=False),
    StructField("timestamp",      StringType(),  nullable=True),   # ISO-8601 문자열
    StructField("amount",         DoubleType(),  nullable=True),
    StructField("merchant_id",    StringType(),  nullable=True),
    StructField("merchant_cat",   StringType(),  nullable=True),
    StructField("location_lat",   DoubleType(),  nullable=True),
    StructField("location_lon",   DoubleType(),  nullable=True),
    StructField("channel",        StringType(),  nullable=True),
    StructField("is_fraud",       BooleanType(), nullable=True),
])


def build_spark_session() -> SparkSession:
    """Kerberos + Iceberg + Ozone 설정이 포함된 SparkSession을 생성합니다."""
    jaas_conf = (
        f"com.sun.security.auth.module.Krb5LoginModule required "
        f"useKeyTab=true storeKey=true "
        f"keyTab=\"{KAFKA_KEYTAB}\" "
        f"principal=\"{KAFKA_PRINCIPAL}\";"
    )

    spark = (
        SparkSession.builder
        .appName("SBI-RawTransactionIngest")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        # Kafka Kerberos
        .config("spark.kafka.sasl.jaas.config", jaas_conf)
        .config("spark.kafka.security.protocol", "SASL_SSL")
        .config("spark.kafka.sasl.mechanism", "GSSAPI")
        .config("spark.kafka.sasl.kerberos.service.name", "kafka")
        .config("spark.kafka.ssl.truststore.location", KAFKA_TRUSTSTORE)
        .config("spark.kafka.ssl.truststore.password", KAFKA_TRUSTSTORE_PW)
        # Kerberos 갱신
        .config("spark.kerberos.keytab", KAFKA_KEYTAB)
        .config("spark.kerberos.principal", KAFKA_PRINCIPAL)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession):
    """Kafka 토픽에서 스트림 DataFrame을 생성합니다."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", KAFKA_GROUP_ID)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)
        .option("kafka.security.protocol",        "SASL_SSL")
        .option("kafka.sasl.mechanism",           "GSSAPI")
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("kafka.ssl.truststore.location",  KAFKA_TRUSTSTORE)
        .option("kafka.ssl.truststore.password",  KAFKA_TRUSTSTORE_PW)
        .option("kafka.sasl.jaas.config",
                f"com.sun.security.auth.module.Krb5LoginModule required "
                f"useKeyTab=true storeKey=true "
                f"keyTab=\"{KAFKA_KEYTAB}\" "
                f"principal=\"{KAFKA_PRINCIPAL}\";")
        .load()
    )


def parse_and_enrich(raw_df):
    """
    Kafka 원시 바이트를 파싱하고 메타데이터 컬럼을 추가합니다.

    - value (bytes) → JSON 파싱 → 스키마 적용
    - kafka_offset, kafka_partition, ingested_at, dt 추가
    - 파싱 실패 레코드는 null transaction_id로 필터링
    """
    parsed = (
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
        # 파티션 키: 이벤트 날짜
        .withColumn(
            "dt",
            when(
                col("event_timestamp").isNotNull(),
                col("event_timestamp").cast("date").cast("string")
            ).otherwise(
                current_timestamp().cast("date").cast("string")
            )
        )
        # 파싱 실패 레코드 제외
        .filter(col("transaction_id").isNotNull())
    )
    return parsed


def write_to_iceberg(batch_df, batch_id: int) -> None:
    """
    foreachBatch 핸들러 — 각 마이크로배치를 Iceberg에 씁니다.
    Iceberg append 모드로 기록하며, 파티션 pruning을 위해 dt 컬럼을 유지합니다.
    """
    count = batch_df.count()
    if count == 0:
        logger.info(f"배치 {batch_id}: 데이터 없음, 스킵")
        return

    logger.info(f"배치 {batch_id}: {count}건 Iceberg 저장 시작")

    (
        batch_df.writeTo(ICEBERG_TABLE)
        .option("mergeSchema", "true")
        .append()
    )

    logger.info(f"배치 {batch_id}: {count}건 저장 완료 → {ICEBERG_TABLE}")


def main() -> None:
    logger.info("SBI Raw Transaction Ingest 잡 시작")

    spark = build_spark_session()
    logger.info(f"Spark 버전: {spark.version}")

    raw_stream = read_kafka_stream(spark)
    enriched   = parse_and_enrich(raw_stream)

    query = (
        enriched.writeStream
        .foreachBatch(write_to_iceberg)
        .outputMode(OUTPUT_MODE)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(f"스트리밍 쿼리 시작: {query.id}")
    logger.info(f"체크포인트: {CHECKPOINT_PATH}")
    logger.info(f"트리거 간격: {TRIGGER_INTERVAL}")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("종료 신호 수신, 스트림 정지...")
        query.stop()
    finally:
        spark.stop()
        logger.info("SparkSession 종료 완료")


if __name__ == "__main__":
    main()
