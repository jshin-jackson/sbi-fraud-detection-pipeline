# SBI Realtime Fraud Detection — Demo 가이드

> **대상:** Cloudera를 처음 사용하는 SBI (State Bank of India) 고객  
> **목적:** Cloudera 플랫폼으로 실시간 사기(Fraud) 탐지를 PoC로 시연

---

## 이 Demo는 무엇을 보여주나요?

인도 최대 은행인 SBI에서 매일 수백만 건의 거래가 발생합니다.  
이 중 사기(Fraud) 의심 거래를 **자동으로 탐지하고 데이터 레이크하우스에 저장**하는 것이 이 Demo의 목표입니다.

### 탐지하는 3가지 사기 패턴

| 패턴 | 설명 | 기준 |
|------|------|------|
| **HIGH_AMOUNT** | 단일 거래가 일정 금액 이상 | ₹5,00,000 (5 lakh) 이상 |
| **VELOCITY** | 단시간 내 동일 계좌에서 반복 거래 | 5분 내 3회 이상 |
| **GEO_ANOMALY** | 물리적으로 불가능한 위치 이동 | 500 km 초과 / 30분 이내 |

### 사용하는 Cloudera 제품

```
Kafka  →  Spark Batch (1분 주기)  →  Ozone/Iceberg(Raw)  →  Spark ETL  →  Ozone/Iceberg(Curated)  →  Hue
메시지큐     실시간 적재                  원시 데이터 저장          사기 탐지          정제 데이터 저장              결과 조회
```

| 제품 | 역할 | 버전 |
|------|------|------|
| **Kafka + SMM** | 거래 데이터 실시간 스트림 | CDP 7.3.1 |
| **Spark 3.5** | Kafka 적재 및 사기 탐지 ETL | CDP 7.3.1 (YARN) |
| **Apache Ozone** | 데이터 레이크 저장소 (OFS 프로토콜) | CDP 7.3.1 |
| **Apache Iceberg** | 데이터 레이크하우스 테이블 형식 | 1.5.2 |
| **Hive + Hue** | 탐지 결과 SQL 조회 | CDP 7.3.1 |
| **Ranger** | 보안 정책 (누가 무엇을 볼 수 있는지) | CDP 7.3.1 |

---

## 환경 정보

```
OS       : RHEL 9.6
CM       : Cloudera Manager 7.13.1
CDP      : 7.3.1
네트워크 : Air-gapped (인터넷 차단 환경)
보안     : Kerberos + Auto-TLS + Ranger (전체 활성화)
실행 계정: systest
Keytab   : /opt/cloudera/systest.keytab
Python   : 3.9.x  ← RHEL 9.6 기본 내장, 추가 설치 불필요
```

> **Python 버전 확인:**
> ```bash
> python3 --version   # Python 3.9.x 출력 확인
> ```

---

## 빠른 시작 (전체 흐름 요약)

```
Step 0  Python 환경   venv 생성 + air-gapped 패키지 설치
Step 1  환경 설정     config/env.conf 편집 (호스트명 입력)
Step 2  환경 검증     bash scripts/01_verify_env.sh
Step 3  인프라 구성   bash infra/01_kafka_setup.sh
                      bash infra/02_ozone_setup.sh
                      beeline -f infra/03_iceberg_ddl.sql
Step 4  Ranger 정책   Ranger UI에서 정책 추가/수정
Step 5  CM 설정       HMS hive-site.xml + Spark Safety Valve 설정
Step 6  데이터 생성   python data_gen/generate_transactions.py --rows 10000 --output /tmp/txn.csv
                      python data_gen/kafka_producer.py --input /tmp/txn.csv
                      (또는 즉석 생성: python data_gen/kafka_producer.py --rows 10000)
Step 7  Spark Ingest  bash scripts/02_run_ingest.sh  (또는 cron 1분 주기)
Step 8  Spark ETL     bash scripts/03_run_etl.sh
Step 9  결과 확인     bash scripts/04_run_report.sh  (또는 Hue SQL Editor)
```

---

## 프로젝트 구조

```
sbi-fraud-detection-pipeline/
│
├── config/                             ← [가장 먼저 편집]
│   ├── env.internal.conf               Cloudera 내부 테스트 환경 설정
│   ├── env.uatdev.conf                 SBI UAT/DEV 환경 설정 (CHANGE_ME → 실제 값으로 교체)
│   ├── env.prd.conf                    SBI Production 환경 설정 (CHANGE_ME → 실제 값으로 교체)
│   └── env.conf → env.internal.conf    현재 활성 환경 (symlink, git 제외)
│
├── scripts/
│   ├── 01_verify_env.sh                환경 자동 검증 (Phase 1)
│   ├── 02_run_ingest.sh                Kafka → Raw Iceberg Spark 실행
│   ├── 03_run_etl.sh                   Raw → Curated Spark ETL 실행
│   └── 04_run_report.sh                beeline 리포트 실행 래퍼
│
├── data_gen/
│   ├── generate_transactions.py        사기 패턴 포함 거래 데이터 생성 (SDV)
│   ├── kafka_producer.py               Kafka 직접 전송
│   └── requirements.txt               Python 패키지 목록 + air-gapped 설치 가이드
│
├── spark/
│   ├── stream/
│   │   └── raw_ingest_job.py           Kafka → sbi_raw.transactions (배치, 오프셋 관리)
│   └── etl/
│       ├── fraud_detection_etl.py      Raw → Curated (사기 탐지 룰 적용)
│       └── rules.py                    탐지 룰 모듈 (HIGH_AMOUNT / VELOCITY / GEO_ANOMALY)
│
├── infra/
│   ├── 01_kafka_setup.sh               Kafka 토픽 생성
│   ├── 02_ozone_setup.sh               Ozone 볼륨/버킷 생성
│   ├── 03_iceberg_ddl.sql              Iceberg 테이블 스키마
│   └── 04_ranger_policies.json         Ranger 보안 정책 템플릿
│
├── conf/
│   ├── spark-defaults.conf             Spark 런타임 설정 (CM Safety Valve 정합)
│   ├── spark_iceberg.conf              레거시 참고용
│   └── kafka_jaas.conf                 Kafka Kerberos JAAS
│
├── report/
│   └── fraud_report.sql                Demo 검증 쿼리 7개 (Hive/Impala 호환)
│
└── run_ingest.sh                       루트 래퍼 (cron 호환 — scripts/02_run_ingest.sh 위임)
```

---

## Step 0 — Python 환경 구성 (Air-gapped)

> **핵심 원칙:** `pip download`는 반드시 클러스터와 **동일한 OS인 RHEL 9.6 Bastion 머신**에서 실행합니다.
> macOS 등 다른 OS에서 실행하면 `sdv` 의존성 wheel의 플랫폼 태그가 달라 설치가 실패합니다.

### RHEL 9.6 Bastion 머신에서 (인터넷 연결, 1회만)

```bash
# 빌드 도구 + gssapi 시스템 패키지 설치
sudo yum install -y gcc python3-devel krb5-devel python3-gssapi

# venv 생성 (시스템 gssapi 공유)
python3 -m venv --system-site-packages /tmp/sbi-venv
source /tmp/sbi-venv/bin/activate
pip install --upgrade pip

# 패키지 다운로드
pip download -r data_gen/requirements.txt -d ./wheels/

tar cf sbi-wheels.tar wheels/
scp sbi-wheels.tar systest@<클러스터-호스트>:/tmp/
```

### 클러스터 노드에서 (오프라인 설치)

```bash
# 시스템 패키지 설치 (gssapi는 yum으로 설치 — pip wheels 불필요)
sudo yum install -y gcc python3-devel krb5-devel python3-gssapi

python3 -m venv --system-site-packages /tmp/sbi-venv
source /tmp/sbi-venv/bin/activate

cd /tmp && tar xf sbi-wheels.tar
pip install --no-index --find-links=./wheels/ -r /root/sbi-fraud-detection-pipeline/data_gen/requirements.txt

# 최종 확인
python3 -c "import gssapi, kafka, sdv, pandas, numpy; print('All OK')"
```

> **이후 모든 python 명령은 venv 활성화 후 실행:**
> ```bash
> source /tmp/sbi-venv/bin/activate
> ```

---

## Phase 1 — 환경 설정 및 검증

### 1-1. 설정 파일 편집

`config/env.internal.conf`를 열어서 실제 클러스터 호스트명과 패스워드를 입력합니다.

```bash
# 변경할 항목 (CHANGE_ME 없는지 확인)
KAFKA_BROKERS="실제-브로커1:9093,실제-브로커2:9093,실제-브로커3:9093"
HMS_HOST="실제-HMS-호스트"
HS2_HOST="실제-HS2-호스트"
OZONE_OM_SERVICE_ID="ozone getconf -confKey ozone.om.service.ids 로 확인"
OZONE_OM_ADDRESS="실제-OM-호스트:9862"
OZONE_VOLUME="실제-볼륨명"
TRUSTSTORE_PW="실제-truststore-패스워드"    # ← 반드시 입력
```

> **TRUSTSTORE_PW 확인 방법:**
> ```bash
> sudo cat /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.pw
> ```

> **Ozone OM 서비스 ID 확인:**
> ```bash
> ozone getconf -confKey ozone.om.service.ids
> ```

### 1-2. 환경 검증 실행

```bash
ln -sf config/env.internal.conf config/env.conf   # 최초 1회
source config/env.conf
bash scripts/01_verify_env.sh
```

모든 항목이 `[OK]`이면 다음 Phase로 진행합니다.

**예상 출력:**
```
=== 1. 설정 파일 확인 ===
  [OK]   KAFKA_BROKERS 설정됨
  [OK]   HMS_HOST: ccycloud-1.jshin.root.comops.site
  ...
=== 2. Kerberos 인증 ===
  [OK]   kinit 성공 (systest@ROOT.COMOPS.SITE)
  [OK]   TGT 발급 확인
...
[완료] 모든 환경 검증 통과! Phase 2를 시작하세요.
```

---

## Phase 2 — 인프라 구성

### 2-1. Kafka 토픽 생성

```bash
source config/env.conf
bash infra/01_kafka_setup.sh
```

생성되는 토픽:
- `sbi-transactions-raw` — 거래 원시 데이터 (파티션 6개)
- `sbi-transactions-dlq` — Dead Letter Queue (파티션 2개)

### 2-2. Ozone 볼륨/버킷 생성

```bash
bash infra/02_ozone_setup.sh
```

생성되는 리소스:
- `/${OZONE_VOLUME}/sbi-raw` — Raw 데이터 버킷
- `/${OZONE_VOLUME}/sbi-curated` — Curated 데이터 버킷

> `hive` 계정 권한도 함께 부여됩니다 (Iceberg DDL 실행 시 필요).

### 2-3. Iceberg 테이블 생성

```bash
envsubst '${OZONE_OM_SERVICE_ID} ${OZONE_VOLUME}' < infra/03_iceberg_ddl.sql \
  | beeline -u "${HS2_JDBC_URL}"
```

생성되는 테이블:

| 테이블 | 레이어 | 파티션 | 설명 |
|--------|--------|--------|------|
| `sbi_raw.transactions` | Raw | `dt` | Kafka 원시 이벤트 |
| `sbi_curated.transactions` | Curated | `dt`, `channel` | 사기 플래그 포함 |
| `sbi_curated.fraud_alerts` | Curated | `dt`, `fraud_reason` | 사기 판정 상세 |
| `sbi_curated.fraud_summary` | Curated | `dt` | 시간대/채널별 집계 |

---

## Phase 3 — Ranger 정책 등록

Ranger는 "누가 어떤 데이터에 접근할 수 있는지" 제어하는 보안 시스템입니다.

기존에 정책이 없는 경우 REST API로 임포트:

```bash
source config/env.conf
curl -k -u admin:RANGER_ADMIN_PW \
  -F "file=@infra/04_ranger_policies.json" \
  "https://${HS2_HOST}:6182/service/plugins/policies/importPoliciesFromFile?isOverride=false"
```

> **주의:** `isOverride=true`는 절대 사용하지 마세요 — 기존 정책이 모두 삭제됩니다.

기존 정책이 있는 경우 Ranger UI(`https://<ranger-host>:6182`)에서 직접 수정/추가:

| 서비스 | 정책명 | 리소스 | 권한 |
|--------|--------|--------|------|
| cm_kafka | `sbi-fraud-demo-kafka-access` | `sbi-transactions-raw`, `sbi-transactions-dlq` | All |
| cm_ozone | `sbi-fraud-demo-ozone-access` | `volume=firstvolume`, `bucket=*`, `key=*` | All (systest, hive, impala) |
| cm_hive | `sbi-fraud-demo-hive-access` | `database=sbi_raw,sbi_curated`, `table=*`, `column=*` | All |

> Demo 환경이므로 서비스별 1개 정책으로 단순화했습니다.  
> `hive.metastore.pre.event.listeners`가 비활성화되어 있으므로 Hive URL 정책은 불필요합니다.

---

## Phase 4 — Cloudera Manager 설정

### 4-1. Hive Metastore 설정 (필수)

**CM → Hive → Configuration → HMS hive-site.xml Safety Valve** 에 추가 후 **HMS 재시작**:

```xml
<property>
  <name>hive.metastore.pre.event.listeners</name>
  <value></value>
</property>
```

> `StorageBasedAuthorizationPreEventListener`가 활성화된 경우 Ranger와 무관하게  
> Iceberg 커밋 시 `RWSTORAGE` 권한 오류가 발생합니다. 이 설정으로 비활성화합니다.

### 4-2. Spark 설정

**CM → SPARK3_ON_YARN → Configuration → spark-defaults.conf Safety Valve** 에 추가:

```properties
spark.jars=/opt/cloudera/parcels/CDH/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.7.3.1.600-325.jar,/opt/cloudera/parcels/CDH/jars/spark-sql-kafka-0-10_2.12-3.5.4.7.3.1.600-325.jar,/opt/cloudera/parcels/CDH/jars/kafka-clients-3.4.1.7.3.1.600-325.jar

spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/jars/ozone-filesystem-hadoop3-1.4.0.7.3.1.600-325.jar:/opt/cloudera/parcels/CDH/jars/ozone-filesystem-common-1.4.0.7.3.1.600-325.jar
spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/jars/ozone-filesystem-hadoop3-1.4.0.7.3.1.600-325.jar:/opt/cloudera/parcels/CDH/jars/ozone-filesystem-common-1.4.0.7.3.1.600-325.jar

spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type=hive
spark.sql.catalog.spark_catalog.uri=thrift://<HMS_HOST>:9083
spark.hadoop.hive.metastore.sasl.enabled=true
spark.hadoop.hive.metastore.kerberos.principal=hive/_HOST@<KRB_REALM>

spark.hadoop.fs.ofs.impl=org.apache.hadoop.fs.ozone.RootedOzoneFileSystem
spark.hadoop.ozone.om.service.ids=<OZONE_OM_SERVICE_ID>
spark.hadoop.ozone.om.address.<OZONE_OM_SERVICE_ID>=<OZONE_OM_ADDRESS>
spark.yarn.access.hadoopFileSystems=ofs://<OZONE_OM_SERVICE_ID>

spark.sql.iceberg.merge-on-read.enabled=true
spark.sql.iceberg.handle-timestamp-without-timezone=true
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
```

**Save Changes → Deploy Client Configuration**

> `<HMS_HOST>`, `<KRB_REALM>`, `<OZONE_OM_SERVICE_ID>`, `<OZONE_OM_ADDRESS>`는  
> `config/env.internal.conf`의 값으로 교체합니다.

---

## Phase 5 — 데이터 생성 및 파이프라인 실행

### 5-1. 합성 데이터 생성 (`generate_transactions.py`)

SDV(Synthetic Data Vault) 라이브러리로 합성 거래 데이터를 생성합니다.  
`kafka_producer.py --rows` 옵션을 사용하면 이 단계를 건너뛸 수 있지만,  
파일로 저장해두고 재사용하려면 단독 실행을 권장합니다.

**사용법:**
```
python data_gen/generate_transactions.py [--rows N] [--output PATH] [--format csv|json]
```

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--rows` | 10000 | 생성할 거래 건수 |
| `--output` | `transactions.csv` | 출력 파일 경로 (실행 디렉터리 기준) |
| `--format` | `csv` | 출력 형식 (`csv` 또는 `json`) |

**예제:**
```bash
source /tmp/sbi-venv/bin/activate

# CSV 생성 (기본)
python data_gen/generate_transactions.py --rows 10000 --output /tmp/txn.csv

# JSON 생성
python data_gen/generate_transactions.py --rows 5000 --output /tmp/txn.json --format json
```

**생성 과정 (3단계):**
1. 시드 데이터 2,000건 생성 (정상 거래 95% + 사기 5%)
2. SDV `GaussianCopulaSynthesizer` 학습 후 `--rows`건 합성
3. 사기 패턴 명시 주입

**주입되는 사기 패턴:**

| 패턴 | 건수 | 내용 |
|------|------|------|
| HIGH_AMOUNT | 5건 | ₹6 lakh ~ ₹20 lakh 거래 |
| VELOCITY | 3건 | `ACC_FRAUD_VEL` 계좌, 60초 간격 연속 거래 |
| GEO_ANOMALY | 2건 | `ACC_FRAUD_GEO` 계좌, Delhi → Mumbai 10분 이동 |

> **참고:** `kafka_producer.py --rows` 옵션 사용 시 내부적으로 이 스크립트를 호출하지만  
> 임시 파일(`/tmp/tmpXXX.csv`)을 생성 후 즉시 삭제하므로 파일이 디스크에 남지 않습니다.

---

### 5-2. 합성 데이터 Kafka 전송

```bash
source /tmp/sbi-venv/bin/activate
source config/env.conf

# 방법 A: 파일에서 전송 (generate_transactions.py로 미리 생성한 경우)
python data_gen/kafka_producer.py --input /tmp/txn.csv --rate ${DEMO_RATE}

# 방법 B: 즉석 생성 + 전송 (파일 저장 없이)
python data_gen/kafka_producer.py --rows ${DEMO_ROWS} --rate ${DEMO_RATE}
```

**생성되는 데이터:**
- 정상 거래 (SDV GaussianCopulaSynthesizer로 생성)
- HIGH_AMOUNT 거래 (₹5 lakh 이상)
- VELOCITY 거래 (동일 계좌 5분 내 반복)
- GEO_ANOMALY 거래 (500 km 초과 이동)

### 5-3. Spark Ingest 실행 (Kafka → Raw Iceberg)

```bash
bash scripts/02_run_ingest.sh
```

**cron 등록 (1분마다 자동 실행):**

```bash
crontab -e
# 추가:
* * * * * /root/sbi-fraud-detection-pipeline/run_ingest.sh >> /var/log/sbi-ingest.log 2>&1
```

### 5-4. Spark ETL 실행 (Raw → Curated)

**사용법:**
```
scripts/03_run_etl.sh [YYYY-MM-DD]
```

| 파라미터 | 필수 여부 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `YYYY-MM-DD` | 선택 | 어제 날짜 | ETL 처리할 파티션 날짜 (`dt=YYYY-MM-DD`) |

**예제:**
```bash
# 어제 날짜 자동 처리 (파라미터 생략)
bash scripts/03_run_etl.sh

# 특정 날짜 처리
bash scripts/03_run_etl.sh 2026-06-07

# 과거 날짜 재처리 (backfill)
bash scripts/03_run_etl.sh 2026-06-01
bash scripts/03_run_etl.sh 2026-06-02
bash scripts/03_run_etl.sh 2026-06-03
```

> **참고:** 지정한 날짜(`dt`)에 `sbi_raw.transactions`에 데이터가 없으면  
> `처리할 데이터가 없습니다. ETL 종료.` 메시지가 출력되고 정상 종료됩니다.  
> 이 경우 Step 5-1~5-3(데이터 생성 → Kafka 전송 → Ingest)가 먼저 실행되었는지 확인하세요.

**cron 등록 (매일 새벽 1시 자동 실행):**
```bash
crontab -e
# 추가:
0 1 * * * /root/sbi-fraud-detection-pipeline/scripts/03_run_etl.sh >> /var/log/sbi-etl.log 2>&1
```

### 5-5. 결과 확인

```bash
bash scripts/04_run_report.sh
```

또는 Hue(`https://<HS2_HOST>:8889`) SQL Editor에서 `report/fraud_report.sql` 실행.

**예상 결과:**
```
fraud_type    | fraud_cnt | total_amount_INR
HIGH_AMOUNT   |     42   |   38,500,000
VELOCITY      |     18   |    9,200,000
GEO_ANOMALY   |      7   |    4,100,000
```

### 5-6. Demo 시연 순서 (고객 앞)

```
[1] Cloudera Manager  → 서비스 상태 Green 확인
[2] SMM               → sbi-transactions-raw 메시지 수신율 그래프
[3] YARN ResourceMgr  → Spark Job 실행 중 확인
[4] Hue (Hive)        → sbi_raw.transactions 건수 증가 확인
[5] Spark ETL 실행    → bash scripts/03_run_etl.sh
[6] Hue (Impala)      → fraud_alerts 쿼리 → 사기 탐지 결과 확인
```

---

## 환경 전환

| 파일 | 대상 환경 | 용도 |
|------|-----------|------|
| `config/env.internal.conf` | Cloudera 내부 테스트 | Demo 개발/검증 |
| `config/env.uatdev.conf` | SBI UAT / DEV 클러스터 | 고객 테스트 환경 |
| `config/env.prd.conf` | SBI Production 클러스터 | 실제 운영 환경 |

```bash
# UAT/DEV 환경으로 전환
ln -sf config/env.uatdev.conf config/env.conf
vi config/env.uatdev.conf   # CHANGE_ME 항목 입력

# Production 환경으로 전환
ln -sf config/env.prd.conf config/env.conf
vi config/env.prd.conf      # CHANGE_ME 항목 입력

# 내부 테스트 환경으로 복귀
ln -sf config/env.internal.conf config/env.conf
```

환경 전환 후 공통 실행 순서:

```bash
source config/env.conf
bash scripts/01_verify_env.sh
bash infra/01_kafka_setup.sh
bash infra/02_ozone_setup.sh
envsubst '${OZONE_OM_SERVICE_ID} ${OZONE_VOLUME}' < infra/03_iceberg_ddl.sql \
  | beeline -u "${HS2_JDBC_URL}"
```

---

## Kerberos 인증 방식 안내

이 프로젝트의 모든 컴포넌트는 **kinit + OS TGT** 방식을 사용합니다.

```
kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
  ↓
OS Kerberos 티켓 캐시(ccache)에 TGT 저장
  ↓
각 컴포넌트가 GSSAPI로 TGT 참조하여 자동 인증
```

| 컴포넌트 | 인증 방식 |
|---------|---------|
| `kafka_producer.py` | 스크립트 내 `kinit` 자동 호출 |
| `scripts/02_run_ingest.sh` | `source config/env.conf` 후 `kinit` 자동 호출 |
| `scripts/03_run_etl.sh` | 동일 |
| `scripts/04_run_report.sh` | 동일 |
| Kafka CLI (`infra/*.sh`) | 스크립트 내 `kinit` 자동 호출 |
| Spark (YARN) | `--keytab`, `--principal`로 YARN이 처리 |
| Hue | 브라우저 로그인 시 Kerberos 자동 처리 |

> **TGT 유효시간:** 기본 10시간. Demo가 10시간 이내라면 재인증 불필요.

---

## 문제 해결 가이드

### Python 패키지 설치 실패 (Air-gapped)

```
증상: No matching distribution found
원인: macOS 등 다른 OS에서 pip download 실행
해결: RHEL 9.6 Bastion 머신에서 pip download 재실행
```

### Kerberos 인증 실패

```
증상: kinit: Password incorrect 또는 Cannot find KDC
원인: keytab 파일이 없거나 경로 오류
해결:
  ls -la /opt/cloudera/systest.keytab
  klist -kt /opt/cloudera/systest.keytab
```

### Kafka 연결 실패

```
증상: SASL authentication failed
원인 1: Kerberos TGT 만료 → source config/env.conf && kinit -kt "${KEYTAB}" "${PRINCIPAL}"
원인 2: Ranger Kafka 정책 미적용 → Ranger UI 확인
원인 3: KAFKA_BROKERS 호스트명 오류 → config/env.conf 확인
```

### Executor Ozone 인증 오류

```
증상: Client cannot authenticate via:[TOKEN, KERBEROS]
원인: Spark YARN executor에 Ozone 위임 토큰 미배포
해결: CM Safety Valve에 다음 설정 추가
      spark.yarn.access.hadoopFileSystems=ofs://<OZONE_OM_SERVICE_ID>
```

### Iceberg RWSTORAGE 권한 오류

```
증상: Permission denied: user [systest] does not have [RWSTORAGE] privilege
원인: HMS의 StorageBasedAuthorizationPreEventListener 활성화
해결:
  1. CM → HMS hive-site.xml Safety Valve
     hive.metastore.pre.event.listeners = (빈 값)
  2. Ranger cm_hive → sbi-iceberg-storage-policy
     URL: iceberg://* / 권한: All (RW Storage 포함) / 사용자: systest
```

### HMS 연결 끊김

```
증상: TTransportException: Socket is closed by peer
원인: HMS Kerberos 설정 누락
해결: conf/spark-defaults.conf 확인
      spark.hadoop.hive.metastore.sasl.enabled=true
      spark.hadoop.hive.metastore.kerberos.principal=hive/_HOST@<KRB_REALM>
```

### Ozone 권한 오류 (hive 계정)

```
증상: User hive doesn't have READ permission to access volume
원인: Ozone 볼륨/버킷에 hive 계정 ACL 미설정
해결:
  sudo -u hdfs ozone sh volume addacl /<VOLUME> --acl "user:hive:rwlc"
  sudo -u hdfs ozone sh bucket addacl /<VOLUME>/sbi-raw --acl "user:hive:rwlc"
  sudo -u hdfs ozone sh bucket addacl /<VOLUME>/sbi-curated --acl "user:hive:rwlc"
```

### 처음부터 재처리

```
증상: "신규 메시지 없음, 종료" 또는 데이터 재적재 필요
해결:
  source config/env.conf
  rm -f "${KAFKA_OFFSET_FILE}"
  bash scripts/02_run_ingest.sh
```

---

## 자주 묻는 질문 (FAQ)

**Q: Cloudera를 처음 쓰는데 각 제품이 무엇인가요?**

| 제품 | 쉬운 설명 | 비유 |
|------|----------|------|
| Kafka | 데이터를 잠시 보관하는 대기열 | 우편함 |
| Spark | 데이터를 빠르게 처리하는 엔진 | 분석가 |
| Ozone | 대용량 파일을 저장하는 분산 저장소 | 창고 |
| Iceberg | 테이블 형식으로 데이터를 관리 | 정리된 서랍 |
| Hive/Hue | SQL로 저장된 데이터를 조회 | 도서관 사서 |
| Ranger | 누가 무엇에 접근할 수 있는지 제어 | 경비원 |

**Q: confluent-kafka 대신 kafka-python을 사용하는 이유는?**

`confluent-kafka`는 내부적으로 C 라이브러리(`librdkafka`)를 필요로 합니다.  
Air-gapped RHEL 환경에서는 C 라이브러리 빌드가 어려워 설치가 실패할 수 있습니다.  
`kafka-python`은 순수 Python으로 `pip download` → `--no-index` 방식으로 확실하게 설치됩니다.

**Q: Spark Streaming 대신 Batch를 사용하는 이유는?**

1분 주기 배치는 Spark Streaming과 비교해 운영이 훨씬 단순합니다.  
checkpoint 파일 없이 오프셋 파일로 중복을 방지하며, cron으로 스케줄링할 수 있습니다.  
실시간성이 1분 단위로 충분한 사기 탐지 시나리오에 적합합니다.

**Q: Demo 데이터는 실제 거래 데이터인가요?**

아니요, SDV(Synthetic Data Vault) 라이브러리로 생성한 가상 데이터입니다.  
통계적 분포는 실제와 유사하지만, 실제 고객 정보는 포함되지 않습니다.

**Q: Demo 후 데이터는 어떻게 정리하나요?**

```bash
source config/env.conf

# Kafka 토픽 메시지 초기화 (retention 기반 자동 삭제)
# Iceberg 테이블 데이터 삭제 (Hue에서 실행)
# TRUNCATE TABLE sbi_raw.transactions;
# TRUNCATE TABLE sbi_curated.fraud_alerts;

# 오프셋 파일 삭제
rm -f "${KAFKA_OFFSET_FILE}"
```

---

## 기술 스택 상세

| 항목 | 값 |
|------|-----|
| CDP 버전 | 7.3.1 |
| Spark 버전 | 3.5.x (Scala 2.12) |
| Iceberg 버전 | 1.5.2 |
| Ozone 버전 | 1.4.0 (CDP 내장) |
| Python | **3.9.x** (RHEL 9.6 기본 내장) |
| Kafka 라이브러리 | kafka-python 2.0+ (순수 Python, air-gapped 호환) |
| SDV | 1.9.0+ (GaussianCopulaSynthesizer) |
| 보안 | Kerberos + Auto-TLS + Ranger (전체 활성화) |
| Kerberos 방식 | kinit + OS TGT (GSSAPI) — 전 컴포넌트 동일 |
| 실행 계정 | systest (단일 계정) |
| Keytab 경로 | /opt/cloudera/systest.keytab |
| Kafka 포트 | 9093 (SASL_SSL) |
| HiveServer2 포트 | 10000 |
| Ozone OM 포트 | 9862 |

---

*이 Demo는 Cloudera SBI Fraud Detection PoC 프로젝트입니다.*  
*문의: Cloudera Solutions Engineering Team*
