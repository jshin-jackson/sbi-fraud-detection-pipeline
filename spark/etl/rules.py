"""
사기 탐지 룰 모듈

각 룰은 입력 DataFrame에 'fraud_flag_<rule>' 컬럼을 추가하고,
최종적으로 is_fraud, fraud_reasons, fraud_score 컬럼을 반환합니다.

룰 목록:
    1. HIGH_AMOUNT   — 단일 거래 금액 > 500,000 INR
    2. VELOCITY      — 동일 계좌 5분 내 3건 이상
    3. GEO_ANOMALY   — 이전 거래 대비 500 km 초과 이동 / 30분 이내
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType


# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
HIGH_AMOUNT_THRESHOLD_INR = 500_000.0
VELOCITY_WINDOW_SECONDS   = 300        # 5분
VELOCITY_MAX_TRANSACTIONS = 3
GEO_DISTANCE_KM           = 500.0
GEO_TIME_WINDOW_SECONDS   = 1800       # 30분
EARTH_RADIUS_KM           = 6371.0

FRAUD_REASON_HIGH_AMOUNT  = "HIGH_AMOUNT"
FRAUD_REASON_VELOCITY     = "VELOCITY"
FRAUD_REASON_GEO_ANOMALY  = "GEO_ANOMALY"


# ---------------------------------------------------------------------------
# 룰 1: 고액 거래
# ---------------------------------------------------------------------------

def apply_high_amount_rule(df: DataFrame) -> DataFrame:
    """
    amount > HIGH_AMOUNT_THRESHOLD_INR 이면 HIGH_AMOUNT 플래그를 설정합니다.
    """
    return df.withColumn(
        "fraud_flag_high_amount",
        F.when(
            F.col("amount") > HIGH_AMOUNT_THRESHOLD_INR,
            F.lit(True)
        ).otherwise(F.lit(False))
    )


# ---------------------------------------------------------------------------
# 룰 2: 단시간 다중 거래 (Velocity)
# ---------------------------------------------------------------------------

def apply_velocity_rule(df: DataFrame) -> DataFrame:
    """
    동일 account_id의 5분(300초) 이내 누적 거래 수가
    VELOCITY_MAX_TRANSACTIONS 이상이면 VELOCITY 플래그를 설정합니다.

    Window: account_id 기준, event_timestamp 기준 rangeBetween
    """
    # TIMESTAMP_NTZ → epoch seconds 변환 후 정렬 (Spark 3.5 호환)
    df = df.withColumn(
        "_event_ts_epoch",
        F.unix_timestamp(F.col("event_timestamp").cast("timestamp"))
    )

    window_spec = (
        Window
        .partitionBy("account_id")
        .orderBy(F.col("_event_ts_epoch"))
        .rangeBetween(-VELOCITY_WINDOW_SECONDS, 0)
    )

    df = df.withColumn(
        "txn_count_5min",
        F.count("transaction_id").over(window_spec)
    )

    return df.withColumn(
        "fraud_flag_velocity",
        F.when(
            F.col("txn_count_5min") >= VELOCITY_MAX_TRANSACTIONS,
            F.lit(True)
        ).otherwise(F.lit(False))
    ).drop("_event_ts_epoch")


# ---------------------------------------------------------------------------
# 룰 3: 지리적 이상 탐지 (GEO_ANOMALY)
# ---------------------------------------------------------------------------

def _haversine_udf():
    """
    두 위경도 사이의 Haversine 거리(km)를 계산하는 Spark UDF를 반환합니다.
    """
    import math

    def haversine(lat1, lon1, lat2, lon2):
        if any(v is None for v in [lat1, lon1, lat2, lon2]):
            return None
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(lat1))
             * math.cos(math.radians(lat2))
             * math.sin(dlon / 2) ** 2)
        return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))

    return F.udf(haversine, DoubleType())


def apply_geo_anomaly_rule(df: DataFrame) -> DataFrame:
    """
    직전 거래와의 이동 거리가 GEO_DISTANCE_KM km 초과이고
    시간 차이가 GEO_TIME_WINDOW_SECONDS 이내이면 GEO_ANOMALY 플래그를 설정합니다.

    Window: account_id 기준 시간순 정렬, lag로 이전 거래 위치 참조
    """
    window_spec = (
        Window
        .partitionBy("account_id")
        .orderBy("event_timestamp")
    )

    haversine = _haversine_udf()

    df = (
        df
        .withColumn("prev_lat",  F.lag("location_lat",  1).over(window_spec))
        .withColumn("prev_lon",  F.lag("location_lon",  1).over(window_spec))
        .withColumn("prev_ts",   F.lag("event_timestamp", 1).over(window_spec))
        .withColumn(
            "geo_distance_km",
            haversine(
                F.col("prev_lat"), F.col("prev_lon"),
                F.col("location_lat"), F.col("location_lon")
            )
        )
        .withColumn(
            "time_diff_sec",
            F.when(
                F.col("prev_ts").isNotNull(),
                F.unix_timestamp(F.col("event_timestamp").cast("timestamp"))
                - F.unix_timestamp(F.col("prev_ts").cast("timestamp"))
            ).otherwise(F.lit(None).cast("long"))
        )
    )

    df = df.withColumn(
        "fraud_flag_geo_anomaly",
        F.when(
            (F.col("geo_distance_km") > GEO_DISTANCE_KM)
            & (F.col("time_diff_sec").isNotNull())
            & (F.col("time_diff_sec") <= GEO_TIME_WINDOW_SECONDS)
            & (F.col("time_diff_sec") > 0),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    return df.drop("prev_lat", "prev_lon", "prev_ts")


# ---------------------------------------------------------------------------
# 최종 집계: is_fraud, fraud_reasons, fraud_score
# ---------------------------------------------------------------------------

def aggregate_fraud_flags(df: DataFrame) -> DataFrame:
    """
    개별 룰 플래그를 종합하여 is_fraud, fraud_reasons, fraud_score 컬럼을 생성합니다.

    fraud_score 산출 기준:
        HIGH_AMOUNT  → +0.5
        VELOCITY     → +0.4
        GEO_ANOMALY  → +0.6
    (최대 1.0으로 클리핑)
    """
    df = df.withColumn(
        "fraud_reasons",
        F.concat_ws(
            ",",
            F.when(F.col("fraud_flag_high_amount"),  F.lit(FRAUD_REASON_HIGH_AMOUNT)),
            F.when(F.col("fraud_flag_velocity"),      F.lit(FRAUD_REASON_VELOCITY)),
            F.when(F.col("fraud_flag_geo_anomaly"),   F.lit(FRAUD_REASON_GEO_ANOMALY)),
        )
    )

    df = df.withColumn(
        "fraud_score",
        F.least(
            F.lit(1.0),
            (F.col("fraud_flag_high_amount").cast("double") * 0.5)
            + (F.col("fraud_flag_velocity").cast("double") * 0.4)
            + (F.col("fraud_flag_geo_anomaly").cast("double") * 0.6)
        )
    )

    df = df.withColumn(
        "is_fraud",
        F.col("fraud_score") > 0.0
    )

    return df


# ---------------------------------------------------------------------------
# 메인 진입점: 전체 룰 적용
# ---------------------------------------------------------------------------

def apply_all_rules(df: DataFrame) -> DataFrame:
    """
    DataFrame에 모든 사기 탐지 룰을 순서대로 적용하고
    최종 is_fraud, fraud_reasons, fraud_score 컬럼이 추가된 DataFrame을 반환합니다.

    입력 컬럼 요구사항:
        transaction_id, account_id, event_timestamp (TimestampType),
        amount, location_lat, location_lon
    """
    df = apply_high_amount_rule(df)
    df = apply_velocity_rule(df)
    df = apply_geo_anomaly_rule(df)
    df = aggregate_fraud_flags(df)
    return df
