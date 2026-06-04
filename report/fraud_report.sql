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
-- 1. 일별 사기 현황 요약 (최근 30일)
-- ---------------------------------------------------------------------------
SELECT
    dt                           AS 날짜,
    SUM(total_txn)               AS 총_거래건수,
    SUM(fraud_txn)               AS 사기_건수,
    ROUND(SUM(fraud_amount), 0)  AS 사기_금액_INR,
    ROUND(
        SUM(fraud_txn) * 100.0 / NULLIF(SUM(total_txn), 0), 2
    )                            AS 사기율_PCT
FROM sbi_curated.fraud_summary
GROUP BY dt
ORDER BY dt DESC
LIMIT 30;


-- ---------------------------------------------------------------------------
-- 2. 채널별 사기 비율 (최근 7일)
-- Hive 날짜 함수: date_sub(current_date(), 7)
-- ---------------------------------------------------------------------------
SELECT
    channel                      AS 채널,
    SUM(total_txn)               AS 총_거래건수,
    SUM(fraud_txn)               AS 사기_건수,
    ROUND(
        SUM(fraud_txn) * 100.0 / NULLIF(SUM(total_txn), 0), 2
    )                            AS 사기율_PCT,
    ROUND(SUM(fraud_amount), 0)  AS 사기_총액_INR
FROM sbi_curated.fraud_summary
WHERE dt >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd')
GROUP BY channel
ORDER BY 사기율_PCT DESC;


-- ---------------------------------------------------------------------------
-- 3. 사기 유형별 건수 및 금액
-- ---------------------------------------------------------------------------
SELECT
    fraud_reason                 AS 사기_유형,
    COUNT(*)                     AS 건수,
    ROUND(SUM(amount), 0)        AS 총_금액_INR,
    ROUND(AVG(fraud_score), 4)   AS 평균_사기점수,
    ROUND(MAX(amount), 0)        AS 최대_금액_INR
FROM sbi_curated.fraud_alerts
GROUP BY fraud_reason
ORDER BY 건수 DESC;


-- ---------------------------------------------------------------------------
-- 4. 시간대별 사기 발생 패턴 (최근 7일, 히트맵용)
-- ---------------------------------------------------------------------------
SELECT
    dt                           AS 날짜,
    hour                         AS 시간,
    SUM(fraud_txn)               AS 사기_건수,
    ROUND(AVG(fraud_rate), 4)    AS 평균_사기율_PCT
FROM sbi_curated.fraud_summary
WHERE dt >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd')
GROUP BY dt, hour
ORDER BY dt, hour;


-- ---------------------------------------------------------------------------
-- 5. 고위험 계좌 TOP 10 (사기 건수 기준)
-- COLLECT_SET: Hive 3.x 지원 집합 집계 함수
-- ---------------------------------------------------------------------------
SELECT
    account_id                   AS 계좌ID,
    COUNT(*)                     AS 사기_건수,
    ROUND(SUM(amount), 0)        AS 사기_총액_INR,
    MIN(event_timestamp)         AS 최초_사기_시각,
    MAX(event_timestamp)         AS 최근_사기_시각,
    COLLECT_SET(fraud_reason)    AS 사기_유형목록
FROM sbi_curated.fraud_alerts
GROUP BY account_id
ORDER BY 사기_건수 DESC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- 6. 사기 거래 상세 (최근 100건)
-- ---------------------------------------------------------------------------
SELECT
    fa.alert_id,
    fa.transaction_id,
    fa.account_id,
    fa.event_timestamp           AS 거래_시각,
    fa.amount                    AS 금액_INR,
    fa.channel                   AS 채널,
    fa.fraud_reason              AS 사기_유형,
    fa.fraud_score               AS 사기_점수,
    fa.location_lat              AS 위도,
    fa.location_lon              AS 경도,
    fa.alerted_at                AS 알림_생성_시각
FROM sbi_curated.fraud_alerts fa
ORDER BY fa.alerted_at DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- 7. 실시간 KPI 대시보드용 — 오늘 사기 현황
-- Hive 날짜 함수: date_format(current_date(), 'yyyy-MM-dd')
-- ---------------------------------------------------------------------------
SELECT
    'TODAY'                                          AS 구분,
    COUNT(*)                                         AS 총_거래,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)        AS 사기_건수,
    ROUND(
        SUM(CASE WHEN is_fraud THEN 1.0 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                AS 사기율_PCT,
    ROUND(
        SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 0
    )                                                AS 사기_총액_INR
FROM sbi_curated.transactions
WHERE dt = date_format(current_date(), 'yyyy-MM-dd');
