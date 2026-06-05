-- =============================================================================
-- SBI 사기 탐지 데모 — 리포트 SQL
-- 대상: Hue SQL Editor (HiveServer2 기반)
-- 검증 환경: Cloudera CDP 7.3.1 / Hive 3.x on Iceberg
--
-- Hue 실행 방법:
--   1. Hue (https://hue.sbi.local) 접속
--   2. SQL Editor → 데이터베이스 선택: sbi_curated
--   3. 아래 쿼리를 각각 붙여넣고 실행
--
-- Beeline 실행 방법:
--   beeline -u "jdbc:hive2://ccycloud-1.jshin.root.comops.site:10000/;principal=hive/_HOST@ROOT.COMOPS.SITE;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks;trustStorePassword=zpXWTjeWPjvNDU4mQnDQPQKn50xfVI9HYX12DSc05x3" \
--           -f fraud_report.sql
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1. Daily fraud summary (last 30 days)
-- ---------------------------------------------------------------------------
SELECT
    dt                           AS date,
    SUM(total_txn)               AS total_txn_count,
    SUM(fraud_txn)               AS fraud_count,
    ROUND(SUM(fraud_amount), 0)  AS fraud_amount_INR,
    ROUND(
        SUM(fraud_txn) * 100.0 / NULLIF(SUM(total_txn), 0), 2
    )                            AS fraud_rate_pct
FROM sbi_curated.fraud_summary
GROUP BY dt
ORDER BY dt DESC
LIMIT 30;


-- ---------------------------------------------------------------------------
-- 2. Fraud rate by channel (last 7 days)
-- ---------------------------------------------------------------------------
SELECT
    channel                      AS channel,
    SUM(total_txn)               AS total_txn_count,
    SUM(fraud_txn)               AS fraud_count,
    ROUND(
        SUM(fraud_txn) * 100.0 / NULLIF(SUM(total_txn), 0), 2
    )                            AS fraud_rate_pct,
    ROUND(SUM(fraud_amount), 0)  AS fraud_amount_INR
FROM sbi_curated.fraud_summary
WHERE dt >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd')
GROUP BY channel
ORDER BY fraud_rate_pct DESC;


-- ---------------------------------------------------------------------------
-- 3. Fraud count and amount by type
-- ---------------------------------------------------------------------------
SELECT
    fraud_reason                 AS fraud_type,
    COUNT(*)                     AS count,
    ROUND(SUM(amount), 0)        AS total_amount_INR,
    ROUND(AVG(fraud_score), 4)   AS avg_fraud_score,
    ROUND(MAX(amount), 0)        AS max_amount_INR
FROM sbi_curated.fraud_alerts
GROUP BY fraud_reason
ORDER BY count DESC;


-- ---------------------------------------------------------------------------
-- 4. Hourly fraud pattern (last 7 days, heatmap)
-- ---------------------------------------------------------------------------
SELECT
    dt                           AS date,
    hour                         AS hour,
    SUM(fraud_txn)               AS fraud_count,
    ROUND(AVG(fraud_rate), 4)    AS avg_fraud_rate_pct
FROM sbi_curated.fraud_summary
WHERE dt >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd')
GROUP BY dt, hour
ORDER BY dt, hour;


-- ---------------------------------------------------------------------------
-- 5. Top 10 high-risk accounts (by fraud count)
-- ---------------------------------------------------------------------------
SELECT
    account_id                   AS account_id,
    COUNT(*)                     AS fraud_count,
    ROUND(SUM(amount), 0)        AS fraud_amount_INR,
    MIN(event_timestamp)         AS first_fraud_at,
    MAX(event_timestamp)         AS last_fraud_at,
    COLLECT_SET(fraud_reason)    AS fraud_types
FROM sbi_curated.fraud_alerts
GROUP BY account_id
ORDER BY fraud_count DESC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- 6. Fraud transaction detail (latest 100)
-- ---------------------------------------------------------------------------
SELECT
    fa.alert_id,
    fa.transaction_id,
    fa.account_id,
    fa.event_timestamp           AS txn_timestamp,
    fa.amount                    AS amount_INR,
    fa.channel                   AS channel,
    fa.fraud_reason              AS fraud_type,
    fa.fraud_score               AS fraud_score,
    fa.location_lat              AS latitude,
    fa.location_lon              AS longitude,
    fa.alerted_at                AS alerted_at
FROM sbi_curated.fraud_alerts fa
ORDER BY fa.alerted_at DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- 7. Real-time KPI dashboard — today's fraud summary
-- ---------------------------------------------------------------------------
SELECT
    'TODAY'                                          AS period,
    COUNT(*)                                         AS total_txn,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)        AS fraud_count,
    ROUND(
        SUM(CASE WHEN is_fraud THEN 1.0 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                AS fraud_rate_pct,
    ROUND(
        SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 0
    )                                                AS fraud_amount_INR
FROM sbi_curated.transactions
WHERE dt = date_format(current_date(), 'yyyy-MM-dd');
