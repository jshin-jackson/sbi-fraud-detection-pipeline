-- =============================================================================
-- SBI 사기 탐지 데모 — Iceberg 테이블 DDL
-- 스토리지: Apache Ozone (ofs://)
-- 카탈로그: Hive Metastore (HMS)
-- =============================================================================
-- 실행 방법:
--   beeline -u "jdbc:hive2://ccycloud-1.jshin.root.comops.site:10000/;principal=hive/_HOST@ROOT.COMOPS.SITE;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks;trustStorePassword=zpXWTjeWPjvNDU4mQnDQPQKn50xfVI9HYX12DSc05x3" -f iceberg_ddl.sql
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 0. Ozone 버킷은 사전에 생성되어 있어야 합니다.
--    ozone sh bucket create /firstvolume/sbi-raw
--    ozone sh bucket create /firstvolume/sbi-curated
-- ---------------------------------------------------------------------------


-- =============================================================================
-- RAW 레이어 데이터베이스
-- =============================================================================

CREATE DATABASE IF NOT EXISTS sbi_raw
  COMMENT 'SBI 사기 탐지 — Kafka 원시 이벤트 저장소';
-- 참고: 데이터베이스 기본 위치는 Hive warehouse를 사용합니다.
--       테이블 데이터는 각 CREATE TABLE의 LOCATION(ofs://)에 저장됩니다.


-- ---------------------------------------------------------------------------
-- Raw 거래 테이블 (Kafka 이벤트 원본 그대로 적재)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sbi_raw.transactions (
    transaction_id  STRING   COMMENT 'UUID 거래 식별자',
    account_id      STRING   COMMENT '계좌 ID',
    event_timestamp TIMESTAMP COMMENT '거래 발생 시각 (ISO-8601)',
    amount          DOUBLE   COMMENT '거래 금액 (INR)',
    merchant_id     STRING   COMMENT '가맹점 ID',
    merchant_cat    STRING   COMMENT '가맹점 업종 코드',
    location_lat    DOUBLE   COMMENT '거래 위도',
    location_lon    DOUBLE   COMMENT '거래 경도',
    channel         STRING   COMMENT 'ONLINE | ATM | POS',
    is_fraud_label  BOOLEAN  COMMENT 'SDV 생성 레이블 (검증용)',
    kafka_offset    BIGINT   COMMENT 'Kafka 파티션 오프셋',
    kafka_partition INT      COMMENT 'Kafka 파티션 번호',
    ingested_at     TIMESTAMP COMMENT 'Spark Stream 적재 시각'
)
PARTITIONED BY (dt STRING COMMENT 'YYYY-MM-DD 파티션')
STORED BY ICEBERG
LOCATION 'ofs://ozone1780551922/firstvolume/sbi-raw/transactions'
TBLPROPERTIES (
    'format-version'                    = '2',
    'write.format.default'              = 'parquet',
    'write.parquet.compression-codec'   = 'snappy',
    'write.metadata.compression-codec'  = 'gzip',
    'history.expire.min-snapshots-to-keep' = '10',
    'write.target-file-size-bytes'      = '134217728'
);


-- =============================================================================
-- CURATED 레이어 데이터베이스
-- =============================================================================

CREATE DATABASE IF NOT EXISTS sbi_curated
  COMMENT 'SBI 사기 탐지 — 정제 및 집계 결과 저장소';
-- 참고: 테이블 데이터는 각 CREATE TABLE의 LOCATION(ofs://)에 저장됩니다.


-- ---------------------------------------------------------------------------
-- Curated 거래 테이블 (사기 여부 플래그 포함)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sbi_curated.transactions (
    transaction_id  STRING    COMMENT 'UUID 거래 식별자',
    account_id      STRING    COMMENT '계좌 ID',
    event_timestamp TIMESTAMP COMMENT '거래 발생 시각',
    amount          DOUBLE    COMMENT '거래 금액 (INR)',
    merchant_id     STRING    COMMENT '가맹점 ID',
    merchant_cat    STRING    COMMENT '가맹점 업종 코드',
    location_lat    DOUBLE    COMMENT '거래 위도',
    location_lon    DOUBLE    COMMENT '거래 경도',
    is_fraud        BOOLEAN   COMMENT '사기 여부 (룰 기반 판정)',
    fraud_reasons   STRING    COMMENT '사기 이유 목록 (쉼표 구분)',
    etl_processed_at TIMESTAMP COMMENT 'ETL 처리 시각'
)
PARTITIONED BY (dt STRING COMMENT 'YYYY-MM-DD', channel STRING COMMENT 'ONLINE|ATM|POS')
STORED BY ICEBERG
LOCATION 'ofs://ozone1780551922/firstvolume/sbi-curated/transactions'
TBLPROPERTIES (
    'format-version'                    = '2',
    'write.format.default'              = 'parquet',
    'write.parquet.compression-codec'   = 'snappy',
    'write.upsert.enabled'              = 'true',
    'write.merge.mode'                  = 'merge-on-read'
);


-- ---------------------------------------------------------------------------
-- Fraud Alerts 테이블 (사기 판정 거래 상세)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sbi_curated.fraud_alerts (
    alert_id        STRING    COMMENT 'UUID 알림 식별자',
    transaction_id  STRING    COMMENT '원본 거래 ID',
    account_id      STRING    COMMENT '계좌 ID',
    event_timestamp TIMESTAMP COMMENT '거래 발생 시각',
    amount          DOUBLE    COMMENT '거래 금액 (INR)',
    channel         STRING    COMMENT 'ONLINE | ATM | POS',
    fraud_score     DOUBLE    COMMENT '사기 점수 (0.0 ~ 1.0)',
    location_lat    DOUBLE    COMMENT '거래 위도',
    location_lon    DOUBLE    COMMENT '거래 경도',
    alerted_at      TIMESTAMP COMMENT '알림 생성 시각'
)
PARTITIONED BY (dt STRING COMMENT 'YYYY-MM-DD', fraud_reason STRING COMMENT 'HIGH_AMOUNT | VELOCITY | GEO_ANOMALY')
STORED BY ICEBERG
LOCATION 'ofs://ozone1780551922/firstvolume/sbi-curated/fraud_alerts'
TBLPROPERTIES (
    'format-version'                    = '2',
    'write.format.default'              = 'parquet',
    'write.parquet.compression-codec'   = 'snappy'
);


-- ---------------------------------------------------------------------------
-- Fraud Summary 테이블 (시간대/채널별 집계)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sbi_curated.fraud_summary (
    hour            INT       COMMENT '시각 (0~23)',
    channel         STRING    COMMENT 'ONLINE | ATM | POS',
    total_txn       BIGINT    COMMENT '총 거래 건수',
    fraud_txn       BIGINT    COMMENT '사기 거래 건수',
    fraud_amount    DOUBLE    COMMENT '사기 거래 총액 (INR)',
    fraud_rate      DOUBLE    COMMENT '사기 비율 (%)',
    summarized_at   TIMESTAMP COMMENT '집계 시각'
)
PARTITIONED BY (dt STRING COMMENT '날짜 (YYYY-MM-DD)')
STORED BY ICEBERG
LOCATION 'ofs://ozone1780551922/firstvolume/sbi-curated/fraud_summary'
TBLPROPERTIES (
    'format-version'  = '2',
    'write.format.default' = 'parquet'
);


-- =============================================================================
-- 확인 쿼리
-- =============================================================================
SHOW DATABASES;
SHOW TABLES IN sbi_raw;
SHOW TABLES IN sbi_curated;

DESCRIBE FORMATTED sbi_raw.transactions;
DESCRIBE FORMATTED sbi_curated.fraud_alerts;
