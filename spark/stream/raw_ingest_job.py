"""
Spark Batch Job — Kafka 원시 이벤트 → sbi_raw.transactions (Iceberg)
1분마다 cron으로 스케줄링하여 실행합니다.

실행 방법:
    source config/env.conf
    spark-submit \
      --master yarn \
      --deploy-mode client \
      --principal "${PRINCIPAL}" \
      --keytab "${KEYTAB}" \
      --properties-file conf/spark_iceberg.conf \
      spark/stream/raw_ingest_job.py

cron 등록 (1분마다):
    * * * * * /root/sbi-fraud-detection-pipeline/scripts/02_run_ingest.sh >> /var/log/sbi-ingest.log 2>&1

환경변수:
    KAFKA_BROKERS     Kafka 브로커 주소
    KAFKA_TOPIC       대상 토픽 (기본: sbi-fd-transactions-raw)
    KAFKA_KEYTAB      Kerberos keytab 경로
    KAFKA_PRINCIPAL   Kerberos 주체
    KAFKA_TRUSTSTORE  SSL truststore JKS 경로 (Java Kafka 클라이언트용)
    KAFKA_TRUSTSTORE_PW SSL truststore 패스워드
    OFFSET_FILE       Kafka 오프셋 저장 파일 경로 (기본: /root/sbi-kafka-offsets.json)

# Air-gapped 환경: --packages 사용 불가. conf/spark_iceberg.conf 의 spark.jars 로 로컬 JAR 지정.
# 검증 환경: Python 3.9.21 / OpenJDK 11 / RHEL 9.6 / Cloudera CDP 7.3.1 (Spark 3.5 / Iceberg 1.5.2)
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
# 환경 설정
# ---------------------------------------------------------------------------
KAFKA_BROKERS      = os.environ.get("KAFKA_BROKERS",      "")
KAFKA_TOPIC        = os.environ.get("KAFKA_TOPIC",        "sbi-fd-transactions-raw")
KAFKA_KEYTAB       = os.environ.get("KAFKA_KEYTAB",       os.environ.get("KEYTAB",    ""))
KAFKA_PRINCIPAL    = os.environ.get("KAFKA_PRINCIPAL",    os.environ.get("PRINCIPAL", ""))
KAFKA_TRUSTSTORE   = os.environ.get("KAFKA_TRUSTSTORE",   "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-in_cluster_truststore.jks")
KAFKA_TRUSTSTORE_PW = os.environ.get("KAFKA_TRUSTSTORE_PW", "")

ICEBERG_TABLE = "sbi_raw.transactions"
OFFSET_FILE   = os.environ.get("OFFSET_FILE", "/root/sbi-kafka-offsets.json")


# ---------------------------------------------------------------------------
# Kafka 이벤트 JSON 스키마
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
    저장된 오프셋 파일을 읽어 startingOffsets JSON 문자열을 반환합니다.
    파일이 없으면 "earliest"를 반환합니다.
    """
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE) as f:
            offsets = json.load(f)
        logger.info(f"오프셋 파일 로드: {offsets}")
        return json.dumps(offsets)
    logger.info("오프셋 파일 없음 — earliest부터 읽기 시작")
    return "earliest"


def save_offsets(raw_df) -> None:
    """처리한 배치의 최대 오프셋 + 1을 파일에 저장합니다."""
    rows = (
        raw_df.groupBy("partition")
        .agg(spark_max("offset").alias("offset"))
        .collect()
    )
    offsets = {KAFKA_TOPIC: {str(row["partition"]): row["offset"] + 1 for row in rows}}
    with open(OFFSET_FILE, "w") as f:
        json.dump(offsets, f)
    logger.info(f"오프셋 저장 완료: {offsets}")


def build_spark_session() -> SparkSession:
    """Kerberos + Iceberg 설정이 포함된 SparkSession을 생성합니다."""
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
    """Kafka 토픽에서 배치 DataFrame을 읽습니다."""
    # JAAS 설정을 문자열로 직접 전달 — 파일 배포 불필요, 모든 executor에서 동작
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
    """Kafka 원시 바이트를 파싱하고 메타데이터 컬럼을 추가합니다."""
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
    logger.info("SBI Raw Transaction Ingest Batch 잡 시작")

    starting_offsets = load_starting_offsets()
    spark = build_spark_session()
    logger.info(f"Spark 버전: {spark.version}")

    raw_df = read_kafka_batch(spark, starting_offsets)

    total = raw_df.count()
    if total == 0:
        logger.info("신규 메시지 없음, 종료")
        spark.stop()
        sys.exit(0)

    logger.info(f"Kafka에서 {total}건 읽기 완료")

    # 오프셋 저장 (다음 실행 시 중복 방지)
    save_offsets(raw_df)

    enriched = parse_and_enrich(raw_df)
    write_count = enriched.count()

    logger.info(f"{write_count}건 Iceberg 저장 시작 → {ICEBERG_TABLE}")
    (
        enriched.writeTo(ICEBERG_TABLE)
        .option("mergeSchema", "true")
        .append()
    )
    logger.info(f"저장 완료: {write_count}건 → {ICEBERG_TABLE}")

    spark.stop()
    logger.info("SparkSession 종료 완료")


if __name__ == "__main__":
    main()
