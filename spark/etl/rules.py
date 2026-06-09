"""
Fraud detection rules module

Each rule adds a 'fraud_flag_<rule>' column to the input DataFrame.
The final output includes is_fraud, fraud_reasons, and fraud_score columns.

Rules:
    1. HIGH_AMOUNT   — single transaction amount > 500,000 INR
    2. VELOCITY      — 3 or more transactions from the same account within 5 minutes
    3. GEO_ANOMALY   — movement of more than 500 km from the previous transaction within 30 minutes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HIGH_AMOUNT_THRESHOLD_INR = 500_000.0
VELOCITY_WINDOW_SECONDS   = 300        # 5 minutes
VELOCITY_MAX_TRANSACTIONS = 3
GEO_DISTANCE_KM           = 500.0
GEO_TIME_WINDOW_SECONDS   = 1800       # 30 minutes
EARTH_RADIUS_KM           = 6371.0

FRAUD_REASON_HIGH_AMOUNT  = "HIGH_AMOUNT"
FRAUD_REASON_VELOCITY     = "VELOCITY"
FRAUD_REASON_GEO_ANOMALY  = "GEO_ANOMALY"


# ---------------------------------------------------------------------------
# Rule 1: High-amount transaction
# ---------------------------------------------------------------------------

def apply_high_amount_rule(df: DataFrame) -> DataFrame:
    """
    Sets the HIGH_AMOUNT flag when amount > HIGH_AMOUNT_THRESHOLD_INR.
    """
    return df.withColumn(
        "fraud_flag_high_amount",
        F.when(
            F.col("amount") > HIGH_AMOUNT_THRESHOLD_INR,
            F.lit(True)
        ).otherwise(F.lit(False))
    )


# ---------------------------------------------------------------------------
# Rule 2: High-frequency transactions (Velocity)
# ---------------------------------------------------------------------------

def apply_velocity_rule(df: DataFrame) -> DataFrame:
    """
    Sets the VELOCITY flag when the cumulative transaction count within
    5 minutes (300 seconds) for the same account_id reaches
    VELOCITY_MAX_TRANSACTIONS or more.

    Window: rangeBetween ordered by event_timestamp, partitioned by account_id
    """
    # Convert TIMESTAMP_NTZ to epoch seconds for ordering (Spark 3.5 compatible)
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
# Rule 3: Geographic anomaly detection (GEO_ANOMALY)
# ---------------------------------------------------------------------------

def _haversine_udf():
    """
    Returns a Spark UDF that computes the Haversine distance (km) between two lat/lon pairs.
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
    Sets the GEO_ANOMALY flag when the distance from the previous transaction
    exceeds GEO_DISTANCE_KM km and the time difference is within
    GEO_TIME_WINDOW_SECONDS seconds.

    Window: ordered by event_timestamp, partitioned by account_id; uses lag to reference previous location
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
# Final aggregation: is_fraud, fraud_reasons, fraud_score
# ---------------------------------------------------------------------------

def aggregate_fraud_flags(df: DataFrame) -> DataFrame:
    """
    Aggregates individual rule flags into is_fraud, fraud_reasons, and fraud_score columns.

    fraud_score calculation:
        HIGH_AMOUNT  → +0.5
        VELOCITY     → +0.4
        GEO_ANOMALY  → +0.6
    (clipped to a maximum of 1.0)
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
# Main entry point: apply all rules
# ---------------------------------------------------------------------------

def apply_all_rules(df: DataFrame) -> DataFrame:
    """
    Applies all fraud detection rules to the DataFrame in sequence and returns
    a DataFrame with is_fraud, fraud_reasons, and fraud_score columns added.

    Required input columns:
        transaction_id, account_id, event_timestamp (TimestampType),
        amount, location_lat, location_lon
    """
    df = apply_high_amount_rule(df)
    df = apply_velocity_rule(df)
    df = apply_geo_anomaly_rule(df)
    df = aggregate_fraud_flags(df)
    return df
