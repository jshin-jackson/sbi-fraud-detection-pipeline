-- =============================================================================
-- SBI Fraud Detection Demo — Iceberg Table DDL
-- Storage : Apache Ozone (ofs://)
-- Catalog : Hive Metastore (HMS)
-- =============================================================================
-- Execution (substitute environment variables with envsubst, then pipe to beeline):
--   source config/env.conf
--   envsubst '${OZONE_OM_SERVICE_ID} ${OZONE_VOLUME}' < infra/03_iceberg_ddl.sql \
--     | beeline -u "${HS2_JDBC_URL}"
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 0. Ozone buckets must be created beforehand.
--    ozone sh bucket create /${OZONE_VOLUME}/sbi-raw
--    ozone sh bucket create /${OZONE_VOLUME}/sbi-curated
-- ---------------------------------------------------------------------------


-- =============================================================================
-- RAW layer database
-- =============================================================================

CREATE DATABASE IF NOT EXISTS sbi_raw
  COMMENT 'SBI Fraud Detection — raw Kafka event store';
-- Note: the database default location uses the Hive warehouse.
--       Table data is stored at the LOCATION (ofs://) specified in each CREATE TABLE.


-- ---------------------------------------------------------------------------
-- Raw transactions table (stores Kafka events as-is)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sbi_raw.transactions (
    transaction_id  STRING   COMMENT 'UUID transaction identifier',
    account_id      STRING   COMMENT 'Account ID',
    event_timestamp TIMESTAMP COMMENT 'Transaction timestamp (ISO-8601)',
    amount          DOUBLE   COMMENT 'Transaction amount (INR)',
    merchant_id     STRING   COMMENT 'Merchant ID',
    merchant_cat    STRING   COMMENT 'Merchant category code',
    location_lat    DOUBLE   COMMENT 'Transaction latitude',
    location_lon    DOUBLE   COMMENT 'Transaction longitude',
    channel         STRING   COMMENT 'ONLINE | ATM | POS',
    is_fraud_label  BOOLEAN  COMMENT 'SDV-generated label (for validation)',
    kafka_offset    BIGINT   COMMENT 'Kafka partition offset',
    kafka_partition INT      COMMENT 'Kafka partition number',
    ingested_at     TIMESTAMP COMMENT 'Spark ingest timestamp'
)
PARTITIONED BY (dt STRING COMMENT 'YYYY-MM-DD partition')
STORED BY ICEBERG
LOCATION 'ofs://${OZONE_OM_SERVICE_ID}/${OZONE_VOLUME}/sbi-raw/transactions'
TBLPROPERTIES (
    'format-version'                    = '2',
    'write.format.default'              = 'parquet',
    'write.parquet.compression-codec'   = 'snappy',
    'write.metadata.compression-codec'  = 'gzip',
    'history.expire.min-snapshots-to-keep' = '10',
    'write.target-file-size-bytes'      = '134217728'
);


-- =============================================================================
-- CURATED layer database
-- =============================================================================

CREATE DATABASE IF NOT EXISTS sbi_curated
  COMMENT 'SBI Fraud Detection — enriched and aggregated result store';
-- Note: Table data is stored at the LOCATION (ofs://) specified in each CREATE TABLE.


-- ---------------------------------------------------------------------------
-- Curated transactions table (includes fraud flag)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sbi_curated.transactions (
    transaction_id  STRING    COMMENT 'UUID transaction identifier',
    account_id      STRING    COMMENT 'Account ID',
    event_timestamp TIMESTAMP COMMENT 'Transaction timestamp',
    amount          DOUBLE    COMMENT 'Transaction amount (INR)',
    merchant_id     STRING    COMMENT 'Merchant ID',
    merchant_cat    STRING    COMMENT 'Merchant category code',
    location_lat    DOUBLE    COMMENT 'Transaction latitude',
    location_lon    DOUBLE    COMMENT 'Transaction longitude',
    is_fraud        BOOLEAN   COMMENT 'Fraud flag (rule-based determination)',
    fraud_reasons   STRING    COMMENT 'List of fraud reasons (comma-separated)',
    etl_processed_at TIMESTAMP COMMENT 'ETL processing timestamp'
)
PARTITIONED BY (dt STRING COMMENT 'YYYY-MM-DD', channel STRING COMMENT 'ONLINE|ATM|POS')
STORED BY ICEBERG
LOCATION 'ofs://${OZONE_OM_SERVICE_ID}/${OZONE_VOLUME}/sbi-curated/transactions'
TBLPROPERTIES (
    'format-version'                    = '2',
    'write.format.default'              = 'parquet',
    'write.parquet.compression-codec'   = 'snappy',
    'write.upsert.enabled'              = 'true',
    'write.merge.mode'                  = 'merge-on-read'
);


-- ---------------------------------------------------------------------------
-- Fraud Alerts table (fraud transaction details)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sbi_curated.fraud_alerts (
    alert_id        STRING    COMMENT 'UUID alert identifier',
    transaction_id  STRING    COMMENT 'Original transaction ID',
    account_id      STRING    COMMENT 'Account ID',
    event_timestamp TIMESTAMP COMMENT 'Transaction timestamp',
    amount          DOUBLE    COMMENT 'Transaction amount (INR)',
    channel         STRING    COMMENT 'ONLINE | ATM | POS',
    fraud_score     DOUBLE    COMMENT 'Fraud score (0.0 ~ 1.0)',
    location_lat    DOUBLE    COMMENT 'Transaction latitude',
    location_lon    DOUBLE    COMMENT 'Transaction longitude',
    alerted_at      TIMESTAMP COMMENT 'Alert creation timestamp'
)
PARTITIONED BY (dt STRING COMMENT 'YYYY-MM-DD', fraud_reason STRING COMMENT 'HIGH_AMOUNT | VELOCITY | GEO_ANOMALY')
STORED BY ICEBERG
LOCATION 'ofs://${OZONE_OM_SERVICE_ID}/${OZONE_VOLUME}/sbi-curated/fraud_alerts'
TBLPROPERTIES (
    'format-version'                    = '2',
    'write.format.default'              = 'parquet',
    'write.parquet.compression-codec'   = 'snappy'
);


-- ---------------------------------------------------------------------------
-- Fraud Summary table (aggregated by hour and channel)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sbi_curated.fraud_summary (
    hour            INT       COMMENT 'Hour of day (0-23)',
    channel         STRING    COMMENT 'ONLINE | ATM | POS',
    total_txn       BIGINT    COMMENT 'Total transaction count',
    fraud_txn       BIGINT    COMMENT 'Fraud transaction count',
    fraud_amount    DOUBLE    COMMENT 'Total fraud amount (INR)',
    fraud_rate      DOUBLE    COMMENT 'Fraud rate (%)',
    summarized_at   TIMESTAMP COMMENT 'Aggregation timestamp'
)
PARTITIONED BY (dt STRING COMMENT 'Date (YYYY-MM-DD)')
STORED BY ICEBERG
LOCATION 'ofs://${OZONE_OM_SERVICE_ID}/${OZONE_VOLUME}/sbi-curated/fraud_summary'
TBLPROPERTIES (
    'format-version'  = '2',
    'write.format.default' = 'parquet'
);


-- =============================================================================
-- Verification queries
-- =============================================================================
SHOW DATABASES;
SHOW TABLES IN sbi_raw;
SHOW TABLES IN sbi_curated;

DESCRIBE FORMATTED sbi_raw.transactions;
DESCRIBE FORMATTED sbi_curated.fraud_alerts;
