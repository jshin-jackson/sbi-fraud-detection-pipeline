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
| 리포트 | Hue SQL Editor (HiveServer2) |

---

## 클러스터 구성

| 호스트 | 역할 |
|---|---|
| ccycloud-1.jshin.root.comops.site | Kafka, Ozone OM, HiveServer2, Ranger, Spark (드라이버) |
| ccycloud-2.jshin.root.comops.site | Kafka, Ozone DataNode |
| ccycloud-3.jshin.root.comops.site | Kafka, Ozone DataNode |
| ccycloud-4.jshin.root.comops.site | Spark History Server |

---

## 사전 준비

### 0. 에어갭 환경 — Python 패키지 준비

**RHEL 9.6에는 Python 3.9가 기본 포함됩니다. SCL 불필요.**

```bash
# [인터넷 가능한 Bastion 머신에서]
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

의존 패키지 (`data_gen/requirements.txt`):
- `sdv>=1.9.0`, `pandas>=1.5.0`, `numpy>=1.23.0`
- `kafka-python>=2.0.0`, `gssapi>=1.8.0`, `python-snappy>=0.7.0`

### 1. Kerberos Principal 및 Keytab 발급

```bash
kadmin.local -q "addprinc -randkey systest@ROOT.COMOPS.SITE"
kadmin.local -q "ktadd -k /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE"
```

keytab은 **모든 클러스터 노드**에 배포합니다:

```bash
scp /opt/cloudera/systest.keytab root@ccycloud-2.jshin.root.comops.site:/opt/cloudera/
scp /opt/cloudera/systest.keytab root@ccycloud-3.jshin.root.comops.site:/opt/cloudera/
scp /opt/cloudera/systest.keytab root@ccycloud-4.jshin.root.comops.site:/opt/cloudera/
```

### 2. Ozone 볼륨 및 버킷 생성

```bash
# firstvolume 볼륨 생성
sudo -u hdfs ozone sh volume create /firstvolume --user systest

# 버킷 생성 (FILE_SYSTEM_OPTIMIZED: OFS 필수 레이아웃)
sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /firstvolume/sbi-raw
sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /firstvolume/sbi-curated

# systest 권한 부여
sudo -u hdfs ozone sh volume addacl /firstvolume --acl "user:systest:rwlc"
sudo -u hdfs ozone sh volume addacl /firstvolume --acl "user:hive:rwlc"
sudo -u hdfs ozone sh bucket addacl /firstvolume/sbi-raw --acl "user:hive:rwlc"
sudo -u hdfs ozone sh bucket addacl /firstvolume/sbi-curated --acl "user:hive:rwlc"

# 생성 확인
ozone sh bucket list /firstvolume
```

> `hive` 계정 권한은 Iceberg DDL 실행 시 HMS가 LOCATION을 검증할 때 필요합니다.

### 3. Kafka 토픽 생성

```bash
kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
bash infra/kafka_setup.sh
```

### 4. Apache Ranger 정책 등록

기존에 정책이 없는 경우 REST API로 임포트:

```bash
curl -k -u admin:RANGER_ADMIN_PW \
  -F "file=@infra/ranger_policies.json" \
  "https://ccycloud-1.jshin.root.comops.site:6182/service/plugins/policies/importPoliciesFromFile?isOverride=false"
```

> `isOverride=true` 는 **절대 사용하지 마세요** — 기존 모든 정책이 삭제됩니다.

기존 정책이 있는 경우 Ranger UI에서 직접 수정:

| 서비스 | 정책명 | 수정 내용 |
|---|---|---|
| `cm_ozone` | `sbi-ozone-raw-policy` | volume: `firstvolume` |
| `cm_ozone` | `sbi-ozone-curated-policy` | volume: `firstvolume` |
| `cm_hive` | `sbi-hive-url-policy` | URL: `ofs://ozone1780551922/firstvolume/sbi-raw/*` 등 |

**Iceberg 메타데이터 커밋용 정책 추가 필수:**

Ranger UI → `cm_hive` → 새 정책 추가:

| 항목 | 값 |
|---|---|
| Policy Name | `sbi-iceberg-storage-policy` |
| Resource Type | `url` |
| URL | `iceberg://*` |
| Recursive | ✅ |
| Users | `systest` |
| Permissions | `All` (RW Storage 포함) |

> Iceberg가 HMS에 스냅샷 메타데이터를 커밋할 때 `iceberg://db/table?snapshot=...` URL에 대한 `RWSTORAGE` 권한이 필요합니다.

### 5. Iceberg 테이블 생성

```bash
beeline -u "jdbc:hive2://ccycloud-1.jshin.root.comops.site:10000/;principal=hive/_HOST@ROOT.COMOPS.SITE;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks;trustStorePassword=zpXWTjeWPjvNDU4mQnDQPQKn50xfVI9HYX12DSc05x3" -f infra/iceberg_ddl.sql
```

### 6. Cloudera Manager 설정

#### 6-1. Hive Metastore 설정

**CM → Hive → Configuration → "Hive Metastore Server Advanced Configuration Snippet (Safety Valve) for hive-site.xml"** 에 추가 후 **HMS 재시작**:

```xml
<property>
  <name>hive.metastore.pre.event.listeners</name>
  <value></value>
</property>
```

> `StorageBasedAuthorizationPreEventListener`가 활성화된 경우 Ranger와 무관하게 Iceberg 커밋 시 `RWSTORAGE` 권한 오류가 발생합니다. 이 설정으로 비활성화합니다.

#### 6-2. Spark 설정

**CM → SPARK3_ON_YARN → Configuration → `spark-defaults.conf` Safety Valve** 에 추가:

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

> - `spark.yarn.access.hadoopFileSystems`: executor 시작 전 Ozone 위임 토큰을 사전 수집합니다. 이 설정 없이는 executor가 OFS에 쓸 때 `CLIENT cannot authenticate via:[TOKEN, KERBEROS]` 오류가 발생합니다.
> - `extraJavaOptions`(--add-opens, JAAS)는 CM이 자동 관리하므로 Safety Valve에 추가하지 않습니다.

**Save Changes → Deploy Client Configuration**

---

## 실행 방법

### Step 1 — 합성 데이터 생성 및 Kafka 전송

```bash
cd /root/sbi-realtime-fraud-detection
source /tmp/sbi-venv/bin/activate

# CSV 생성 후 Kafka 전송 (100건/초)
python data_gen/generate_transactions.py --rows 10000 --output /tmp/transactions.csv
python data_gen/kafka_producer.py --input /tmp/transactions.csv --rate 100

# 또는 즉시 생성 + 전송
python data_gen/kafka_producer.py --rows 5000 --rate 50
```

Kafka 환경변수 (기본값이 설정되어 있으므로 변경 시에만 설정):

```bash
export KAFKA_BROKERS=ccycloud-1.jshin.root.comops.site:9093,...
export KAFKA_TOPIC=sbi-transactions-raw
export KAFKA_KEYTAB=/opt/cloudera/systest.keytab
export KAFKA_PRINCIPAL=systest@ROOT.COMOPS.SITE
export KAFKA_CA_PEM=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem
```

### Step 2 — Spark Batch Job 실행 (Kafka → Raw Iceberg)

```bash
cd /root/sbi-realtime-fraud-detection
chmod +x run_ingest.sh
./run_ingest.sh
```

`run_ingest.sh` 가 수행하는 작업:
1. `kinit` — Kerberos TGT 갱신 (Kafka SASL 및 Ozone 인증용)
2. `HADOOP_CONF_DIR` / `YARN_CONF_DIR` 설정
3. `SPARK_CLASSPATH` — Ozone filesystem JAR 로드 (driver JVM용)
4. `spark-submit` — CM 관리 conf + `--properties-file`로 커스텀 설정 적용

**cron 등록 (1분마다 자동 실행):**

```bash
crontab -e
# 아래 줄 추가:
* * * * * /root/sbi-realtime-fraud-detection/run_ingest.sh >> /var/log/sbi-ingest.log 2>&1
```

**처음부터 재처리 (Kafka earliest부터):**

```bash
rm -f /root/sbi-kafka-offsets.json
./run_ingest.sh
```

### Step 3 — Spark ETL (Raw → Curated Iceberg)

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --principal systest@ROOT.COMOPS.SITE \
  --keytab /opt/cloudera/systest.keytab \
  --properties-file conf/spark-defaults.conf \
  --py-files spark/etl/rules.py \
  spark/etl/fraud_detection_etl.py
```

### Step 4 — Hue에서 리포트 확인

Hue(`https://ccycloud-1.jshin.root.comops.site:8889`) SQL Editor에서 `report/fraud_report.sql` 실행.

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
│   ├── kafka_producer.py             # Kafka 전송 (kafka-python, Kerberos SASL_SSL)
│   └── requirements.txt             # Python 의존성 (kafka-python, gssapi, python-snappy 등)
├── spark/
│   ├── stream/
│   │   └── raw_ingest_job.py         # Spark Batch Job — Kafka → sbi_raw.transactions
│   └── etl/
│       ├── fraud_detection_etl.py    # Raw Iceberg → 룰 적용 → Curated Iceberg
│       └── rules.py                  # 사기 탐지 룰 (HIGH_AMOUNT, VELOCITY, GEO_ANOMALY)
├── report/
│   └── fraud_report.sql              # Hue SQL Editor용 리포트 쿼리
├── infra/
│   ├── kafka_setup.sh                # Kafka 토픽 생성
│   ├── ozone_setup.sh                # Ozone 볼륨/버킷 생성 (firstvolume)
│   ├── iceberg_ddl.sql               # Iceberg 테이블 DDL (ofs:// LOCATION)
│   └── ranger_policies.json          # Ranger 정책 (firstvolume 기반)
├── conf/
│   ├── spark-defaults.conf           # Spark 커스텀 설정 (--properties-file로 전달)
│   ├── spark_iceberg.conf            # 레거시 참고용
│   └── kafka_jaas.conf               # Kafka Kerberos JAAS (useKeyTab)
├── run_ingest.sh                     # Spark Batch Job 실행 스크립트 (kinit 포함)
└── README.md
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
kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
klist
```

### `HADOOP_CONF_DIR must be set` 오류

`run_ingest.sh`를 사용하지 않고 직접 `spark-submit` 실행 시 발생합니다.

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
```

또는 `run_ingest.sh`를 통해 실행하세요.

### Kafka Kerberos 로그인 오류

```
LoginException: the client is being asked for a password
```

`kinit`이 실행되지 않은 경우입니다. `run_ingest.sh`는 자동으로 `kinit`을 수행합니다.
직접 실행 시:

```bash
kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
```

### Ozone `ClassNotFoundException: RootedOzoneFs`

`SPARK_CLASSPATH`에 Ozone JAR이 없는 경우입니다. `run_ingest.sh`는 자동 설정됩니다.
직접 실행 시:

```bash
export SPARK_CLASSPATH="/opt/cloudera/parcels/CDH/jars/ozone-filesystem-hadoop3-1.4.0.7.3.1.600-325.jar:/opt/cloudera/parcels/CDH/jars/ozone-filesystem-common-1.4.0.7.3.1.600-325.jar"
```

### Executor Ozone 인증 오류 (TOKEN, KERBEROS)

```
AccessControlException: Client cannot authenticate via:[TOKEN, KERBEROS]
```

Executor 컨테이너에 Ozone 위임 토큰이 배포되지 않은 경우입니다.  
`conf/spark-defaults.conf` 및 CM Safety Valve에 아래 설정이 있는지 확인하세요:

```properties
spark.yarn.access.hadoopFileSystems=ofs://ozone1780551922
```

### Iceberg RWSTORAGE 권한 오류

```
Permission denied: user [systest] does not have [RWSTORAGE] privilege
on [iceberg://sbi_raw/transactions?snapshot=...]
```

두 가지 설정이 모두 필요합니다:

**1. Ranger `sbi-iceberg-storage-policy` 확인** (`cm_hive` 서비스):
- URL: `iceberg://*` (Recursive)
- Users: `systest`
- Permissions: All (RW Storage 포함)

**2. HMS `hive.metastore.pre.event.listeners` 비활성화** (CM → Hive Metastore → hive-site.xml Safety Valve):

```xml
<property>
  <name>hive.metastore.pre.event.listeners</name>
  <value></value>
</property>
```

설정 후 HMS 재시작 필수.

> `StorageBasedAuthorizationPreEventListener`가 활성화되면 Ranger URL 정책과 무관하게 자체 권한 체크를 수행하여 항상 DENIED됩니다.

### Iceberg `s3a://` 경로 오류

이전에 `s3a://` LOCATION으로 생성된 테이블이 남아있는 경우입니다.

```bash
beeline -u "..." -e "DROP TABLE IF EXISTS sbi_raw.transactions; DROP TABLE IF EXISTS sbi_curated.transactions; DROP TABLE IF EXISTS sbi_curated.fraud_alerts; DROP TABLE IF EXISTS sbi_curated.fraud_summary;"
beeline -u "..." -f infra/iceberg_ddl.sql
```

### Ozone 권한 오류 (hive 계정)

```
User hive doesn't have READ permission to access volume Volume:firstvolume
```

```bash
sudo -u hdfs ozone sh volume addacl /firstvolume --acl "user:hive:rwlc"
sudo -u hdfs ozone sh bucket addacl /firstvolume/sbi-raw --acl "user:hive:rwlc"
sudo -u hdfs ozone sh bucket addacl /firstvolume/sbi-curated --acl "user:hive:rwlc"
```

### HMS 연결 끊김 (Socket is closed by peer)

```
TTransportException: Socket is closed by peer
```

HMS Kerberos 인증 설정이 없는 경우입니다. `conf/spark-defaults.conf`에 확인:

```properties
spark.hadoop.hive.metastore.sasl.enabled=true
spark.hadoop.hive.metastore.kerberos.principal=hive/_HOST@ROOT.COMOPS.SITE
```

### 처음부터 재처리 (Kafka earliest)

```bash
rm -f /root/sbi-kafka-offsets.json
./run_ingest.sh
```

### JAR 파일 경로 확인

```bash
find /opt/cloudera/parcels/CDH/jars/ -name "iceberg-spark-runtime*.jar"
find /opt/cloudera/parcels/CDH/jars/ -name "spark-sql-kafka*.jar"
find /opt/cloudera/parcels/CDH/jars/ -name "ozone-filesystem-hadoop3*.jar"
```

### Iceberg 스냅샷 정리

```sql
CALL spark_catalog.system.expire_snapshots(
  'sbi_raw.transactions',
  TIMESTAMP '2024-01-01 00:00:00',
  10
);
```
