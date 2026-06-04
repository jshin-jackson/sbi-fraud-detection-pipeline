# SBI 실시간 사기 탐지 데모

Cloudera CDP 7.3.1 온프레미스 **에어갭(Air-gapped)** 환경에서 동작하는 엔드-투-엔드 사기 탐지 데모입니다.

```
SDV → Kafka → Spark Stream → Ozone(Iceberg Raw) → Spark ETL → Ozone(Iceberg Curated) → Hue(Report)
```

> **에어갭 환경 원칙**: 인터넷 연결 없이 동작합니다. Maven/PyPI 원격 다운로드를 사용하지 않으며,
> 모든 JAR과 Python wheel은 사전에 준비된 로컬 파일을 사용합니다.

## 기술 스택

| 구성 요소 | 버전 / 제품 |
|---|---|
| Cloudera CDP | 7.3.1 온프레미스 (에어갭) |
| OS | RHEL 9.6 |
| JDK | OpenJDK 11 |
| Apache Kafka | Cloudera SMM 내장 Kafka |
| Apache Spark | 3.5.x / Scala 2.12 (YARN 실행) |
| Apache Iceberg | 1.5.2 (CDP 파슬 내장, `iceberg-spark-runtime-3.5_2.12`) |
| Apache Ozone | CDP 7.3.1 내장 (S3G HTTPS 포트 9879) |
| 보안 | Kerberos + Auto-TLS + Apache Ranger |
| 데이터 생성 | Python 3.9.21 + SDV 1.9.0 |
| 리포트 | Hue SQL Editor (HiveServer2) |

---

## 사전 준비

### 0. 에어갭 환경 — 패키지 사전 준비

인터넷이 가능한 별도 머신(Bastion 등)에서 아래 파일을 미리 준비하여 클러스터로 복사합니다.

#### Python 패키지 (wheel)

**RHEL 9.6에는 Python 3.9가 기본 포함되어 있습니다. SCL 불필요.**

Python 3 버전 확인:

```bash
python3 --version   # 3.9.21 확인
```

wheel 다운로드 및 설치:

```bash
# [Bastion 머신에서 — Python 3.8+ 환경]
python3 -m venv /tmp/sbi-venv
source /tmp/sbi-venv/bin/activate
pip install --upgrade pip
pip download -r data_gen/requirements.txt -d ./wheels/
tar czf sbi-wheels.tar.gz wheels/

# [클러스터 노드로 복사 후]
python3 -m venv /tmp/sbi-venv
source /tmp/sbi-venv/bin/activate
tar xzf sbi-wheels.tar.gz
pip install --no-index --find-links=./wheels/ -r data_gen/requirements.txt
```

#### Spark JAR 확인 (CDP 파슬 기준)

CDP 7.3.1 파슬에는 Iceberg 및 Kafka Connector JAR이 사전 포함되어 있습니다.
아래 경로에서 파일 존재 여부를 확인합니다:

```bash
ls /opt/cloudera/parcels/CDH/jars/ | grep -E "iceberg|spark-sql-kafka|kafka-clients"
```

파일명이 다른 경우 `conf/spark_iceberg.conf` 의 `spark.jars` 경로를 실제 경로로 수정하세요.

---

### 1. Kerberos Principal 및 Keytab 발급

```bash
kadmin.local -q "addprinc -randkey systest@ROOT.COMOPS.SITE"

kadmin.local -q "ktadd -k /root/systest.keytab systest@ROOT.COMOPS.SITE"
```

### 2. Ozone 버킷 생성

```bash
kinit -kt /root/systest.keytab systest@ROOT.COMOPS.SITE
bash infra/ozone_setup.sh
```

### 3. Kafka 토픽 생성

```bash
kinit -kt /root/systest.keytab systest@ROOT.COMOPS.SITE
bash infra/kafka_setup.sh
```

### 4. Apache Ranger 정책 등록

Ranger UI (`https://ccycloud-1.jshin.root.comops.site:6182`) 또는 REST API로 `infra/ranger_policies.json` 임포트:

```bash
curl -X POST \
  -u admin:RANGER_ADMIN_PW \
  -H "Content-Type: multipart/form-data" \
  -F "file=@infra/ranger_policies.json" \
  "https://ccycloud-1.jshin.root.comops.site:6182/service/plugins/policies/importPoliciesFromFile?isOverride=false"
```

### 5. Iceberg 테이블 생성

Beeline(Hive) 또는 Hue SQL Editor에서 `infra/iceberg_ddl.sql` 내용을 실행합니다.

```bash
# Beeline 실행 예시
beeline -u "jdbc:hive2://ccycloud-1.jshin.root.comops.site:10000/;principal=hive/_HOST@ROOT.COMOPS.SITE;ssl=true" \
        -f infra/iceberg_ddl.sql
```

---

## 실행 방법

### Step 1 — SDV 합성 데이터 생성 및 Kafka 전송

```bash
cd data_gen

# Python 3 가상환경 활성화 (사전 준비)
source /tmp/sbi-venv/bin/activate

# Python 패키지 설치 (사전 준비된 wheel 사용, Python 3.8+ 필수)
pip install --no-index --find-links=/path/to/wheels/ -r requirements.txt

# CSV 파일 생성 후 Kafka 전송 (100건/초)
python generate_transactions.py --rows 10000 --output /tmp/transactions.csv
python kafka_producer.py --input /tmp/transactions.csv --rate 100

# 또는 즉시 생성 + 전송 (50건/초)
python kafka_producer.py --rows 5000 --rate 50
```

환경변수 설정 (실제 클러스터 값으로 수정):

```bash
export KAFKA_BROKERS=ccycloud-1.jshin.root.comops.site:9093,ccycloud-2.jshin.root.comops.site:9093,ccycloud-3.jshin.root.comops.site:9093
export KAFKA_TOPIC=sbi_transactions_raw
export KAFKA_KEYTAB=/root/systest.keytab
export KAFKA_PRINCIPAL=systest@ROOT.COMOPS.SITE
export KAFKA_TRUSTSTORE=/etc/security/certs/truststore.jks
export KAFKA_TRUSTSTORE_PW=changeit
```

### Step 2 — Spark Structured Streaming (Raw Iceberg 적재)

에어갭 환경이므로 `--packages` 대신 `conf/spark_iceberg.conf`의 `spark.jars`로 로컬 JAR을 참조합니다.

```bash
kinit -kt /root/systest.keytab systest@ROOT.COMOPS.SITE

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --principal systest@ROOT.COMOPS.SITE \
  --keytab /root/systest.keytab \
  --properties-file conf/spark_iceberg.conf \
  spark/stream/raw_ingest_job.py
```

스트리밍 잡은 백그라운드에서 계속 실행됩니다.

- YARN ResourceManager UI: `https://ccycloud-1.jshin.root.comops.site:8090`
- Spark History Server UI: `https://ccycloud-4.jshin.root.comops.site:18089`

### Step 3 — Spark ETL (사기 탐지 + Curated Iceberg 저장)

```bash
# 특정 날짜 처리
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --principal systest@ROOT.COMOPS.SITE \
  --keytab /root/systest.keytab \
  --properties-file conf/spark_iceberg.conf \
  --py-files spark/etl/rules.py \
  spark/etl/fraud_detection_etl.py --dt 2024-06-15

# 기본값: 어제 날짜 자동 처리
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --principal systest@ROOT.COMOPS.SITE \
  --keytab /root/systest.keytab \
  --properties-file conf/spark_iceberg.conf \
  --py-files spark/etl/rules.py \
  spark/etl/fraud_detection_etl.py
```

### Step 4 — Hue에서 리포트 쿼리 실행

Hue(`https://ccycloud-1.jshin.root.comops.site:8889`) SQL Editor에서 `report/fraud_report.sql` 내용을 붙여넣고 실행합니다.

리포트 쿼리 목록:

| 번호 | 내용 |
|---|---|
| 1 | 일별 사기 현황 요약 |
| 2 | 채널별 사기 비율 (최근 7일) |
| 3 | 사기 유형별 건수 및 금액 |
| 4 | 시간대별 사기 발생 패턴 |
| 5 | 고위험 계좌 TOP 10 |
| 6 | 사기 거래 상세 (최근 100건) |
| 7 | 오늘 사기 현황 KPI |

---

## 디렉터리 구조

```
sbi-realtime-fraud-detection/
├── data_gen/
│   ├── generate_transactions.py      # SDV 합성 데이터 생성
│   ├── kafka_producer.py             # Kafka 전송 (Kerberos SASL_SSL)
│   └── requirements.txt             # Python 의존성 (에어갭 설치 방법 포함)
├── spark/
│   ├── stream/
│   │   └── raw_ingest_job.py         # Spark Structured Streaming → Raw Iceberg
│   └── etl/
│       ├── fraud_detection_etl.py    # Raw Iceberg → 룰 적용 → Curated Iceberg
│       └── rules.py                  # 사기 탐지 룰 (HIGH_AMOUNT, VELOCITY, GEO_ANOMALY)
├── report/
│   └── fraud_report.sql              # Hue SQL Editor용 리포트 쿼리
├── infra/
│   ├── kafka_setup.sh                # Kafka 토픽 생성
│   ├── ozone_setup.sh                # Ozone 버킷 생성
│   ├── iceberg_ddl.sql               # Iceberg 테이블 DDL (Raw + Curated)
│   └── ranger_policies.json          # Ranger 정책 템플릿
├── conf/
│   ├── kafka_kerberos.properties     # Kafka JAAS / SASL 설정
│   ├── spark_iceberg.conf            # Spark + Iceberg + Ozone 설정 (로컬 JAR 경로 포함)
│   └── krb5.conf.example             # Kerberos 설정 예시
└── README.md
```

---

## Iceberg 테이블 구조

### Raw 레이어 (`s3a://sbi-raw/`)

| 테이블 | 파티션 | 설명 |
|---|---|---|
| `sbi_raw.transactions` | `dt` | Kafka 원시 이벤트 전체 |

### Curated 레이어 (`s3a://sbi-curated/`)

| 테이블 | 파티션 | 설명 |
|---|---|---|
| `sbi_curated.transactions` | `dt`, `channel` | 정제된 거래 + 사기 플래그 |
| `sbi_curated.fraud_alerts` | `dt`, `fraud_reason` | 사기 판정 거래 상세 |
| `sbi_curated.fraud_summary` | `dt` | 시간대/채널별 집계 |

---

## 사기 탐지 룰

| 룰 ID | 조건 | fraud_reason |
|---|---|---|
| HIGH_AMOUNT | `amount > 500,000 INR` | `HIGH_AMOUNT` |
| VELOCITY | 동일 계좌 5분 내 3건 이상 | `VELOCITY` |
| GEO_ANOMALY | 이전 거래 대비 500 km 초과 이동 / 30분 이내 | `GEO_ANOMALY` |

---

## 보안 구성 요약

| 구성 요소 | 인증 | 암호화 | 권한 관리 |
|---|---|---|---|
| Kafka | Kerberos GSSAPI | SASL_SSL (Auto-TLS) | Ranger Kafka 정책 |
| Ozone | Kerberos | S3A + Auto-TLS | Ranger Ozone 정책 |
| Hive/Hue | Kerberos | SSL (Auto-TLS) | Ranger Hive 정책 |
| YARN | Kerberos | Wire encryption | Ranger YARN 정책 |

---

## 트러블슈팅

### Kerberos 티켓 만료

```bash
kinit -kt /root/systest.keytab systest@ROOT.COMOPS.SITE
klist
```

### JAR 파일 경로 오류

CDP 파슬 JAR 실제 경로 확인:

```bash
find /opt/cloudera/parcels/CDH/jars/ -name "iceberg-spark-runtime*.jar"
find /opt/cloudera/parcels/CDH/jars/ -name "spark-sql-kafka*.jar"
find /opt/cloudera/parcels/CDH/jars/ -name "kafka-clients*.jar"
```

확인된 경로로 `conf/spark_iceberg.conf`의 `spark.jars` 값을 수정합니다.

### Ozone S3A 인증 키 발급

Auto-TLS + Kerberos 환경에서 Ozone S3 게이트웨이 접근 키 발급:

```bash
kinit -kt /root/systest.keytab systest@ROOT.COMOPS.SITE
ozone s3 getsecret
# 출력된 accessKey / secret 을 conf/spark_iceberg.conf 에 설정
```

### Java 11 모듈 오류

`InaccessibleObjectException` 발생 시 `conf/spark_iceberg.conf`의
`spark.driver.extraJavaOptions` / `spark.executor.extraJavaOptions` 설정이
적용되었는지 확인합니다.

### Ozone S3A 연결 오류

```bash
# S3 게이트웨이 접근 키 확인
ozone s3 getsecret

# S3G 포트 접근 확인
curl -k https://ccycloud-1.jshin.root.comops.site:9879/
```

### Iceberg 스냅샷 정리 (유지 관리)

Hue SQL Editor 또는 Beeline에서 실행:

```sql
CALL spark_catalog.system.expire_snapshots(
  'sbi_raw.transactions',
  TIMESTAMP '2024-01-01 00:00:00',
  10
);
```
