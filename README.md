# SBI 실시간 사기 탐지 데모

Cloudera CDP 7.3.1 온프레미스 **에어갭(Air-gapped)** 환경에서 동작하는 엔드-투-엔드 사기 탐지 데모입니다.

```
SDV → Kafka → Spark Batch (1분 주기) → Ozone/Iceberg(Raw) → Spark ETL → Ozone/Iceberg(Curated) → Hue(Report)
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
| Apache Iceberg | 1.5.2 (CDP 파슬 내장) |
| Apache Ozone | CDP 7.3.1 내장 (OFS `ofs://` 프로토콜) |
| 보안 | Kerberos + Auto-TLS + Apache Ranger |
| 데이터 생성 | Python 3.9.21 + SDV 1.9.0 |
| Kafka Python 클라이언트 | kafka-python 2.x (confluent-kafka 미사용) |
| 리포트 | Hue SQL Editor (Hive/Impala) |

---

## 클러스터 구성

| 호스트 | 역할 |
|---|---|
| ccycloud-1.jshin.root.comops.site | Kafka, Ozone OM, HiveServer2, Ranger, Spark (드라이버) |
| ccycloud-2.jshin.root.comops.site | Kafka, Ozone DataNode |
| ccycloud-3.jshin.root.comops.site | Kafka, Ozone DataNode |
| ccycloud-4.jshin.root.comops.site | Spark History Server |

---

## 디렉터리 구조

```
sbi-realtime-fraud-detection/
├── config/                           # ★ 환경 설정 (AML 프로젝트와 동일 패턴)
│   ├── env.internal.conf             #   Cloudera 내부 테스트 환경
│   ├── env.customer.conf             #   SBI 고객 환경 (CHANGE_ME 플레이스홀더)
│   └── env.conf                      #   symlink → 활성 환경 (git 제외)
├── scripts/                          # ★ 실행 스크립트
│   ├── verify_env.sh                 #   Phase 1: 전체 환경 자동 검증
│   ├── run_ingest.sh                 #   Kafka → Raw Iceberg Spark Job
│   ├── run_etl.sh                    #   Raw → Curated Spark ETL
│   └── run_report.sh                 #   beeline 리포트 실행
├── data_gen/
│   ├── generate_transactions.py      #   SDV 합성 데이터 생성
│   ├── kafka_producer.py             #   Kafka 전송 (kafka-python, Kerberos SASL_SSL)
│   └── requirements.txt             #   Python 의존성
├── spark/
│   ├── stream/
│   │   └── raw_ingest_job.py         #   Spark Batch Job — Kafka → sbi_raw.transactions
│   └── etl/
│       ├── fraud_detection_etl.py    #   Raw Iceberg → 룰 적용 → Curated Iceberg
│       └── rules.py                  #   사기 탐지 룰 (HIGH_AMOUNT, VELOCITY, GEO_ANOMALY)
├── report/
│   └── fraud_report.sql              #   Hue SQL Editor용 리포트 (Hive/Impala 호환)
├── infra/
│   ├── 01_kafka_setup.sh             #   Kafka 토픽 생성
│   ├── 02_ozone_setup.sh             #   Ozone 볼륨/버킷 생성
│   ├── 03_iceberg_ddl.sql            #   Iceberg 테이블 DDL
│   └── 04_ranger_policies.json       #   Ranger 정책
├── conf/
│   ├── spark-defaults.conf           #   Spark 설정 (CM Safety Valve 정합)
│   ├── spark_iceberg.conf            #   레거시 참고용
│   └── kafka_jaas.conf               #   Kafka Kerberos JAAS
├── run_ingest.sh                     #   루트 래퍼 (cron 호환성 — scripts/ 위임)
└── README.md
```

---

## 빠른 시작 (Phase 0~5)

### Phase 0. 환경 설정 연결

```bash
cd /root/sbi-realtime-fraud-detection

# 내부 테스트 환경
ln -sf config/env.internal.conf config/env.conf

# 고객 환경으로 전환 시
# ln -sf config/env.customer.conf config/env.conf
# → config/env.customer.conf의 CHANGE_ME 값을 실제 값으로 수정 후 전환
```

### Phase 1. 환경 검증

```bash
source config/env.conf
bash scripts/verify_env.sh
```

**모든 항목이 OK여야 다음 Phase로 진행합니다.**

### Phase 2. 인프라 설정

```bash
source config/env.conf

# 2-1. Kafka 토픽 생성
bash infra/01_kafka_setup.sh

# 2-2. Ozone 볼륨/버킷 생성
bash infra/02_ozone_setup.sh

# 2-3. Iceberg 테이블 생성
beeline -u "${HS2_JDBC_URL}" -f infra/03_iceberg_ddl.sql
```

### Phase 3. Ranger 정책 등록

기존에 정책이 없는 경우 REST API로 임포트:

```bash
source config/env.conf
curl -k -u admin:RANGER_ADMIN_PW \
  -F "file=@infra/04_ranger_policies.json" \
  "https://${HS2_HOST}:6182/service/plugins/policies/importPoliciesFromFile?isOverride=false"
```

> `isOverride=true` 는 **절대 사용하지 마세요** — 기존 모든 정책이 삭제됩니다.

기존 정책이 있는 경우 Ranger UI에서 직접 수정:

| 서비스 | 정책명 | 수정 내용 |
|---|---|---|
| `cm_ozone` | `sbi-ozone-raw-policy` | volume: `firstvolume` |
| `cm_ozone` | `sbi-ozone-curated-policy` | volume: `firstvolume` |
| `cm_hive` | `sbi-hive-url-policy` | URL: `ofs://ozone1780551922/firstvolume/...` |
| `cm_hive` | `sbi-iceberg-storage-policy` | URL: `iceberg://*`, RW Storage |

### Phase 4. Cloudera Manager 설정

#### 4-1. Hive Metastore 설정 (필수)

**CM → Hive → Configuration → HMS hive-site.xml Safety Valve** 에 추가 후 **HMS 재시작**:

```xml
<property>
  <name>hive.metastore.pre.event.listeners</name>
  <value></value>
</property>
```

#### 4-2. Spark 설정

**CM → SPARK3_ON_YARN → Configuration → spark-defaults.conf Safety Valve** 에 추가:

```properties
spark.jars=/opt/cloudera/parcels/CDH/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.7.3.1.600-325.jar,/opt/cloudera/parcels/CDH/jars/spark-sql-kafka-0-10_2.12-3.5.4.7.3.1.600-325.jar,/opt/cloudera/parcels/CDH/jars/kafka-clients-3.4.1.7.3.1.600-325.jar

spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/jars/ozone-filesystem-hadoop3-1.4.0.7.3.1.600-325.jar:/opt/cloudera/parcels/CDH/jars/ozone-filesystem-common-1.4.0.7.3.1.600-325.jar
spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/jars/ozone-filesystem-hadoop3-1.4.0.7.3.1.600-325.jar:/opt/cloudera/parcels/CDH/jars/ozone-filesystem-common-1.4.0.7.3.1.600-325.jar

spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type=hive
spark.sql.catalog.spark_catalog.uri=thrift://ccycloud-1.jshin.root.comops.site:9083
spark.hadoop.hive.metastore.sasl.enabled=true
spark.hadoop.hive.metastore.kerberos.principal=hive/_HOST@ROOT.COMOPS.SITE

spark.hadoop.fs.ofs.impl=org.apache.hadoop.fs.ozone.RootedOzoneFileSystem
spark.hadoop.ozone.om.service.ids=ozone1780551922
spark.hadoop.ozone.om.address.ozone1780551922=ccycloud-1.jshin.root.comops.site:9862
spark.yarn.access.hadoopFileSystems=ofs://ozone1780551922

spark.sql.iceberg.merge-on-read.enabled=true
spark.sql.iceberg.handle-timestamp-without-timezone=true
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
```

**Save Changes → Deploy Client Configuration**

### Phase 5. 데이터 생성 및 파이프라인 실행

#### 5-1. 합성 데이터 생성 및 Kafka 전송

```bash
source config/env.conf
source /tmp/sbi-venv/bin/activate

python data_gen/kafka_producer.py --rows ${DEMO_ROWS} --rate ${DEMO_RATE}
```

#### 5-2. Spark Ingest 실행 (Kafka → Raw Iceberg)

```bash
bash scripts/run_ingest.sh

# 처음부터 재처리 시
rm -f ${KAFKA_OFFSET_FILE}
bash scripts/run_ingest.sh
```

**cron 등록 (1분마다):**

```bash
crontab -e
# 추가:
* * * * * /root/sbi-realtime-fraud-detection/run_ingest.sh >> /var/log/sbi-ingest.log 2>&1
```

#### 5-3. Spark ETL 실행 (Raw → Curated)

```bash
bash scripts/run_etl.sh              # 어제 날짜 자동 처리
bash scripts/run_etl.sh 2024-01-07   # 특정 날짜 처리
```

#### 5-4. 리포트 확인

```bash
bash scripts/run_report.sh
```

또는 Hue(`https://ccycloud-1.jshin.root.comops.site:8889`) SQL Editor에서 `report/fraud_report.sql` 실행.

---

## Python 패키지 사전 준비 (에어갭)

```bash
# [인터넷 가능한 Bastion 머신에서]
python3 -m venv /tmp/sbi-venv
source /tmp/sbi-venv/bin/activate
pip download -r data_gen/requirements.txt -d ./wheels/
tar czf sbi-wheels.tar.gz wheels/

# [클러스터 노드로 복사 후]
python3 -m venv /tmp/sbi-venv
source /tmp/sbi-venv/bin/activate
tar xzf sbi-wheels.tar.gz
pip install --no-index --find-links=./wheels/ -r data_gen/requirements.txt
```

---

## Iceberg 테이블 구조

### Raw 레이어 (`ofs://ozone1780551922/firstvolume/sbi-raw/`)

| 테이블 | 파티션 | 설명 |
|---|---|---|
| `sbi_raw.transactions` | `dt` | Kafka 원시 이벤트 전체 |

### Curated 레이어 (`ofs://ozone1780551922/firstvolume/sbi-curated/`)

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
| Ozone | Kerberos 위임 토큰 | OFS + Auto-TLS | Ranger Ozone 정책 |
| Hive/Hue | Kerberos | SSL (Auto-TLS) | Ranger Hive 정책 |
| YARN | Kerberos | Wire encryption | Ranger YARN 정책 |

---

## 트러블슈팅

### Kerberos 티켓 만료

```bash
source config/env.conf
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
klist
```

### `HADOOP_CONF_DIR must be set` 오류

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
```

### Kafka Kerberos 로그인 오류

```
LoginException: the client is being asked for a password
```

```bash
source config/env.conf
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
```

### Executor Ozone 인증 오류 (TOKEN, KERBEROS)

```
AccessControlException: Client cannot authenticate via:[TOKEN, KERBEROS]
```

CM Safety Valve에 다음 설정이 있는지 확인:

```properties
spark.yarn.access.hadoopFileSystems=ofs://ozone1780551922
```

### Iceberg RWSTORAGE 권한 오류

```
Permission denied: user [systest] does not have [RWSTORAGE] privilege on [iceberg://...]
```

두 가지 설정 모두 필요:

1. **HMS hive-site.xml Safety Valve** — `hive.metastore.pre.event.listeners` 를 빈 값으로 설정 후 HMS 재시작
2. **Ranger `sbi-iceberg-storage-policy`** — URL: `iceberg://*`, RW Storage 권한, systest 사용자

### HMS 연결 끊김 (Socket is closed by peer)

`conf/spark-defaults.conf`에 확인:

```properties
spark.hadoop.hive.metastore.sasl.enabled=true
spark.hadoop.hive.metastore.kerberos.principal=hive/_HOST@ROOT.COMOPS.SITE
```

### Ozone 권한 오류 (hive 계정)

```bash
sudo -u hdfs ozone sh volume addacl /firstvolume --acl "user:hive:rwlc"
sudo -u hdfs ozone sh bucket addacl /firstvolume/sbi-raw --acl "user:hive:rwlc"
sudo -u hdfs ozone sh bucket addacl /firstvolume/sbi-curated --acl "user:hive:rwlc"
```

### 처음부터 재처리 (Kafka earliest)

```bash
source config/env.conf
rm -f "${KAFKA_OFFSET_FILE}"
bash scripts/run_ingest.sh
```

### JAR 파일 경로 확인

```bash
find /opt/cloudera/parcels/CDH/jars/ -name "iceberg-spark-runtime*.jar"
find /opt/cloudera/parcels/CDH/jars/ -name "ozone-filesystem-hadoop3*.jar"
```
