"""
Spark ETL 잡 — sbi_raw.transactions → 사기 탐지 룰 적용 → sbi_curated.*

파이프라인:
    1. sbi_raw.transactions 에서 처리 대상 날짜 파티션 읽기
    2. rules.py 의 3가지 룰 적용 (HIGH_AMOUNT, VELOCITY, GEO_ANOMALY)
    3. 결과를 3개 Iceberg 테이블에 저장:
       - sbi_curated.transactions   : 전체 거래 + 사기 플래그
       - sbi_curated.fraud_alerts   : 사기 판정 거래 상세
       - sbi_curated.fraud_summary  : 시간대/채널별 집계

실행 방법:
    source config/env.conf
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --principal "${PRINCIPAL}" \
      --keytab "${KEYTAB}" \
      --properties-file /path/to/conf/spark_iceberg.conf \
      --py-files spark/etl/rules.py \
      spark/etl/fraud_detection_etl.py --dt 2024-06-15

# Air-gapped 환경: --packages 사용 불가. conf/spark_iceberg.conf 의 spark.jars 로 로컬 JAR 지정.
# 검증 환경: Python 3.9.21 / OpenJDK 11 / RHEL 9.6 / Cloudera CDP 7.3.1 (Spark 3.5 / Iceberg 1.5.2)
"""

import argparse
import logging
import os
import uuid
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# 동일 패키지의 룰 모듈 임포트
import sys
sys.path.insert(0, os.path.dirname(__file__))
from rules import apply_all_rules


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("SBI-FraudETL")


# ---------------------------------------------------------------------------
# 환경 설정
# ---------------------------------------------------------------------------
KEYTAB    = os.environ.get("SPARK_KEYTAB",    os.environ.get("KEYTAB",    ""))
PRINCIPAL = os.environ.get("SPARK_PRINCIPAL", os.environ.get("PRINCIPAL", ""))

RAW_TABLE        = "sbi_raw.transactions"
CURATED_TRANS    = "sbi_curated.transactions"
CURATED_ALERTS   = "sbi_curated.fraud_alerts"
CURATED_SUMMARY  = "sbi_curated.fraud_summary"


# ---------------------------------------------------------------------------
# SparkSession 생성
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
        # Iceberg 최적화
        .config("spark.sql.iceberg.merge-on-read.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Raw 데이터 읽기
# ---------------------------------------------------------------------------

def read_raw(spark: SparkSession, dt: str) -> DataFrame:
    """지정 날짜의 Raw 거래 데이터를 읽습니다."""
    logger.info(f"Raw 데이터 읽기: {RAW_TABLE}, dt={dt}")
    df = (
        spark.table(RAW_TABLE)
        .filter(F.col("dt") == dt)
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
    )
    count = df.count()
    logger.info(f"Raw 레코드 수: {count}건")
    return df


# ---------------------------------------------------------------------------
# Curated 테이블 저장
# ---------------------------------------------------------------------------

def write_curated_transactions(df: DataFrame, dt: str) -> None:
    """사기 플래그가 포함된 전체 거래를 sbi_curated.transactions에 저장합니다."""
    logger.info(f"sbi_curated.transactions 저장 시작 (dt={dt})")

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
    logger.info(f"sbi_curated.transactions 저장 완료")


def write_fraud_alerts(df: DataFrame, dt: str) -> None:
    """사기로 판정된 거래를 개별 fraud_reason별로 sbi_curated.fraud_alerts에 저장합니다."""
    logger.info(f"sbi_curated.fraud_alerts 저장 시작 (dt={dt})")

    gen_alert_id = F.udf(lambda: str(uuid.uuid4()), StringType())

    fraud_df = df.filter(F.col("is_fraud") == True)

    # fraud_reasons를 행으로 explode (멀티 룰 건)
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
    logger.info(f"사기 알림 건수: {alert_count}건")

    (
        alerts.writeTo(CURATED_ALERTS)
        .option("mergeSchema", "true")
        .overwritePartitions()
    )
    logger.info(f"sbi_curated.fraud_alerts 저장 완료")


def write_fraud_summary(df: DataFrame, dt: str) -> None:
    """시간대/채널별 사기 집계를 sbi_curated.fraud_summary에 저장합니다."""
    logger.info(f"sbi_curated.fraud_summary 저장 시작 (dt={dt})")

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
    logger.info(f"sbi_curated.fraud_summary 저장 완료")


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def run_etl(dt: str) -> None:
    logger.info(f"===== SBI Fraud Detection ETL 시작: dt={dt} =====")
    spark = build_spark_session()

    try:
        raw_df = read_raw(spark, dt)

        if raw_df.rdd.isEmpty():
            logger.warning(f"dt={dt} 에 처리할 데이터가 없습니다. ETL 종료.")
            return

        logger.info("사기 탐지 룰 적용 중...")
        scored_df = apply_all_rules(raw_df)
        scored_df.cache()

        fraud_count = scored_df.filter(F.col("is_fraud")).count()
        total_count = scored_df.count()
        logger.info(f"탐지 결과: {fraud_count}/{total_count}건 사기 ({100*fraud_count/total_count:.2f}%)")

        write_curated_transactions(scored_df, dt)
        write_fraud_alerts(scored_df, dt)
        write_fraud_summary(scored_df, dt)

        scored_df.unpersist()

    finally:
        spark.stop()
        logger.info("===== SBI Fraud Detection ETL 완료 =====")


def main() -> None:
    parser = argparse.ArgumentParser(description="SBI Fraud Detection ETL")
    parser.add_argument(
        "--dt",
        type=str,
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="처리 대상 날짜 (YYYY-MM-DD, 기본: 어제)",
    )
    args = parser.parse_args()
    run_etl(args.dt)


if __name__ == "__main__":
    main()
