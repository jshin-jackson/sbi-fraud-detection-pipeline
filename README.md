# SBI Realtime Fraud Detection — Demo Guide

> **Audience:** SBI (State Bank of India) customers new to Cloudera  
> **Purpose:** Demonstrate real-time fraud detection as a PoC on the Cloudera platform

---

## What Does This Demo Show?

SBI, India's largest bank, processes millions of transactions every day.  
The goal of this demo is to **automatically detect suspicious (fraud) transactions and store them in a data lakehouse**.

### 3 Fraud Patterns Detected

| Pattern | Description | Threshold |
|---------|-------------|-----------|
| **HIGH_AMOUNT** | A single transaction exceeds a set amount | ₹5,00,000 (5 lakh) or more |
| **VELOCITY** | Repeated transactions from the same account in a short window | 3 or more within 5 minutes |
| **GEO_ANOMALY** | Physically impossible location movement | More than 500 km within 30 minutes |

### Cloudera Products Used

```
Kafka  →  Spark Batch (1-min interval)  →  Ozone/Iceberg(Raw)  →  Spark ETL  →  Ozone/Iceberg(Curated)  →  Hue
Message queue     Real-time ingest              Raw data storage          Fraud detection     Curated data storage          Result queries
```

| Product | Role | Version |
|---------|------|---------|
| **Kafka + SMM** | Real-time transaction data stream | CDP 7.3.1 |
| **Spark 3.5** | Kafka ingest and fraud detection ETL | CDP 7.3.1 (YARN) |
| **Apache Ozone** | Data lake storage (OFS protocol) | CDP 7.3.1 |
| **Apache Iceberg** | Data lakehouse table format | 1.5.2 |
| **Hive + Hue** | SQL queries on detection results | CDP 7.3.1 |
| **Ranger** | Security policies (who can access what) | CDP 7.3.1 |

---

## Environment Details

```
OS       : RHEL 9.6
CM       : Cloudera Manager 7.13.1
CDP      : 7.3.1
Network  : Air-gapped (no internet access)
Security : Kerberos + Auto-TLS + Ranger (all enabled)
Run as   : systest
Keytab   : /opt/cloudera/systest.keytab
Python   : 3.9.x  ← built into RHEL 9.6, no additional installation required
```

> **Verify Python version:**
> ```bash
> python3 --version   # should output Python 3.9.x
> ```

---

## Quick Start (Full Flow Summary)

```
Step 0  Python environment   create venv + install air-gapped packages
Step 1  Configuration        edit config/env.conf (enter hostnames)
Step 2  Verification         bash scripts/01_verify_env.sh
Step 3  Infrastructure       bash infra/01_kafka_setup.sh
                             bash infra/02_ozone_setup.sh
                             bash infra/03_iceberg_ddl.sh
Step 4  Ranger policies      add/edit policies in Ranger UI
Step 5  CM configuration     HMS hive-site.xml + Spark Safety Valve settings
Step 6  Data generation      python data_gen/generate_transactions.py --rows 10000 --output /tmp/txn.csv
                             python data_gen/kafka_producer.py --input /tmp/txn.csv
                             (or generate on the fly: python data_gen/kafka_producer.py --rows 10000)
Step 7  Spark Ingest         bash scripts/02_run_ingest.sh  (or cron every minute)
Step 8  Spark ETL            bash scripts/03_run_etl.sh
Step 9  Verify results       bash scripts/04_run_report.sh  (or Hue SQL Editor)
```

---

## Project Structure

```
sbi-fraud-detection-pipeline/
│
├── config/                             ← [edit first]
│   ├── env.internal.conf               Cloudera internal test environment configuration
│   ├── env.uatdev.conf                 SBI UAT/DEV environment configuration (replace CHANGE_ME with actual values)
│   ├── env.prd.conf                    SBI Production environment configuration (replace CHANGE_ME with actual values)
│   └── env.conf → env.internal.conf    currently active environment (symlink, excluded from git)
│
├── scripts/
│   ├── 01_verify_env.sh                automated environment verification (Phase 1)
│   ├── 02_run_ingest.sh                Kafka → Raw Iceberg Spark runner
│   ├── 03_run_etl.sh                   Raw → Curated Spark ETL runner
│   └── 04_run_report.sh                beeline report runner wrapper
│
├── data_gen/
│   ├── generate_transactions.py        generate transaction data with fraud patterns (SDV)
│   ├── kafka_producer.py               send data directly to Kafka
│   └── requirements.txt               Python package list + air-gapped installation guide
│
├── spark/
│   ├── stream/
│   │   └── raw_ingest_job.py           Kafka → sbi_raw.transactions (batch, offset management)
│   └── etl/
│       ├── fraud_detection_etl.py      Raw → Curated (apply fraud detection rules)
│       └── rules.py                    detection rules module (HIGH_AMOUNT / VELOCITY / GEO_ANOMALY)
│
├── infra/
│   ├── 01_kafka_setup.sh               create Kafka topics
│   ├── 02_ozone_setup.sh               create Ozone volumes/buckets
│   ├── 03_iceberg_ddl.sql              Iceberg table schema (SQL template)
│   ├── 03_iceberg_ddl.sh               Iceberg DDL runner (envsubst + beeline)
│   └── 04_ranger_policies.json         Ranger security policy template
│
├── conf/
│   ├── spark-defaults.conf             Spark runtime configuration (aligned with CM Safety Valve)
│   ├── spark_iceberg.conf              legacy reference
│   └── kafka_jaas.conf                 Kafka Kerberos JAAS
│
├── report/
│   └── fraud_report.sql                7 demo verification queries (Hive/Impala compatible)
│
└── run_ingest.sh                       root wrapper (cron-compatible — delegates to scripts/02_run_ingest.sh)
```

---

## Step 0 — Python Environment Setup (Air-gapped)

> **Key principle:** Always run `pip download` on an **RHEL 9.6 Bastion machine with internet access that matches the cluster OS**.
> Running it on a different OS (e.g. macOS) will cause installation failures because the `sdv` dependency wheel platform tags will differ.

### On the RHEL 9.6 Bastion machine (internet-connected, one time only)

```bash
# Install build tools + gssapi system packages
sudo yum install -y gcc python3-devel krb5-devel python3-gssapi

# Create venv (sharing system gssapi)
python3 -m venv --system-site-packages /tmp/sbi-venv
source /tmp/sbi-venv/bin/activate
pip install --upgrade pip

# Download packages
pip download -r data_gen/requirements.txt -d ./wheels/

tar cf sbi-wheels.tar wheels/
scp sbi-wheels.tar systest@<cluster-host>:/tmp/
```

### On the cluster node (offline installation)

```bash
# Install system packages (gssapi installed via yum — no pip wheels needed)
# gettext: provides the envsubst command (required to run infra/03_iceberg_ddl.sh)
sudo yum install -y gcc python3-devel krb5-devel python3-gssapi gettext

python3 -m venv --system-site-packages /tmp/sbi-venv
source /tmp/sbi-venv/bin/activate

cd /tmp && tar xf sbi-wheels.tar
pip install --no-index --find-links=./wheels/ -r /root/sbi-fraud-detection-pipeline/data_gen/requirements.txt

# Final verification
python3 -c "import gssapi, kafka, sdv, pandas, numpy; print('All OK')"
```

> **All subsequent python commands must be run with the venv activated:**
> ```bash
> source /tmp/sbi-venv/bin/activate
> ```

---

## Phase 1 — Configuration and Verification

### 1-1. Edit Configuration File

Open `config/env.internal.conf` and enter the actual cluster hostnames and passwords.

```bash
# Items to update (verify no CHANGE_ME values remain)
KAFKA_BROKERS="actual-broker1:9093,actual-broker2:9093,actual-broker3:9093"
HMS_HOST="actual-HMS-host"
HS2_HOST="actual-HS2-host"
OZONE_OM_SERVICE_ID="check with: ozone getconf -confKey ozone.om.service.ids"
OZONE_OM_ADDRESS="actual-OM-host:9862"
OZONE_VOLUME="actual-volume-name"
TRUSTSTORE_PW="actual-truststore-password"    # ← must be set
```

> **How to find TRUSTSTORE_PW:**
> ```bash
> sudo cat /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.pw
> ```

> **How to find Ozone OM service ID:**
> ```bash
> ozone getconf -confKey ozone.om.service.ids
> ```

### 1-2. Run Environment Verification

```bash
ln -sf config/env.internal.conf config/env.conf   # once initially
source config/env.conf
bash scripts/01_verify_env.sh
```

If all items show `[OK]`, proceed to the next Phase.

**Expected output:**
```
=== 1. Configuration file check ===
  [OK]   KAFKA_BROKERS is set
  [OK]   HMS_HOST: ccycloud-1.jshin.root.comops.site
  ...
=== 2. Kerberos authentication ===
  [OK]   kinit succeeded (systest@ROOT.COMOPS.SITE)
  [OK]   TGT issued successfully
...
[DONE] All environment checks passed! Proceed to Phase 2.
```

---

## Phase 2 — Infrastructure Setup

### 2-1. Create Kafka Topics

```bash
source config/env.conf
bash infra/01_kafka_setup.sh
```

Topics created:
- `sbi-fd-transactions-raw` — raw transaction data (6 partitions)
- `sbi-fd-transactions-dlq` — Dead Letter Queue (2 partitions)

### 2-2. Create Ozone Volumes/Buckets

```bash
bash infra/02_ozone_setup.sh
```

Resources created:
- `/${OZONE_VOLUME}/sbi-raw` — Raw data bucket
- `/${OZONE_VOLUME}/sbi-curated` — Curated data bucket

> `hive` account permissions are also granted (required for Iceberg DDL execution).

### 2-3. Create Iceberg Tables

```bash
bash infra/03_iceberg_ddl.sh
```

Tables created:

| Table | Layer | Partition | Description |
|-------|-------|-----------|-------------|
| `sbi_raw.transactions` | Raw | `dt` | Raw Kafka events |
| `sbi_curated.transactions` | Curated | `dt`, `channel` | Includes fraud flag |
| `sbi_curated.fraud_alerts` | Curated | `dt`, `fraud_reason` | Fraud detection details |
| `sbi_curated.fraud_summary` | Curated | `dt` | Aggregated by hour/channel |

---

## Phase 3 — Ranger Policy Registration

Ranger is the security system that controls "who can access what data".

If no policies exist, import via REST API:

```bash
source config/env.conf
curl -k -u admin:RANGER_ADMIN_PW \
  -F "file=@infra/04_ranger_policies.json" \
  "https://${HS2_HOST}:6182/service/plugins/policies/importPoliciesFromFile?isOverride=false"
```

> **Warning:** Never use `isOverride=true` — it will delete all existing policies.

If policies already exist, add/edit them directly in the Ranger UI (`https://<ranger-host>:6182`):

| Service | Policy name | Resource | Permissions |
|---------|-------------|----------|-------------|
| cm_kafka | `sbi-fd-kafka-admin` | `sbi-fd-transactions-raw`, `sbi-fd-transactions-dlq` | All (including delegateAdmin) |
| cm_ozone | `sbi-fraud-demo-ozone-access` | `volume=firstvolume`, `bucket=*`, `key=*` | All (systest, hive, impala) |
| cm_hive | `sbi-fraud-demo-hive-access` | `database=sbi_raw,sbi_curated`, `table=*`, `column=*` | All |

> Simplified to one policy per service for the demo environment.  
> Hive URL policies are not required since `hive.metastore.pre.event.listeners` is disabled.

---

## Phase 4 — Cloudera Manager Configuration

### 4-1. Hive Metastore Configuration (Required)

Add the following to **CM → Hive → Configuration → HMS hive-site.xml Safety Valve**, then **restart HMS**:

```xml
<property>
  <name>hive.metastore.pre.event.listeners</name>
  <value></value>
</property>
```

> If `StorageBasedAuthorizationPreEventListener` is enabled, Iceberg commits will fail with
> an `RWSTORAGE` permission error regardless of Ranger policies. This setting disables it.

### 4-2. Spark Configuration

Add the following to **CM → SPARK3_ON_YARN → Configuration → spark-defaults.conf Safety Valve**:

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

> Replace `<HMS_HOST>`, `<KRB_REALM>`, `<OZONE_OM_SERVICE_ID>`, and `<OZONE_OM_ADDRESS>`  
> with the values from `config/env.internal.conf`.

---

## Phase 5 — Data Generation and Pipeline Execution

### 5-1. Generate Synthetic Data (`generate_transactions.py`)

Uses the SDV (Synthetic Data Vault) library to generate synthetic transaction data.  
You can skip this step by using the `kafka_producer.py --rows` option, but running it separately  
is recommended if you want to save the data to a file for reuse.

**Usage:**
```
python data_gen/generate_transactions.py [--rows N] [--output PATH] [--format csv|json]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--rows` | 10000 | Number of transactions to generate |
| `--output` | `transactions.csv` | Output file path (relative to run directory) |
| `--format` | `csv` | Output format (`csv` or `json`) |

**Examples:**
```bash
source /tmp/sbi-venv/bin/activate

# Generate CSV (default)
python data_gen/generate_transactions.py --rows 10000 --output /tmp/txn.csv

# Generate JSON
python data_gen/generate_transactions.py --rows 5000 --output /tmp/txn.json --format json
```

**Generation process (3 steps):**
1. Generate 2,000 seed records (95% normal transactions + 5% fraud)
2. Train SDV `GaussianCopulaSynthesizer` and synthesize `--rows` records
3. Explicitly inject fraud patterns

**Injected fraud patterns:**

| Pattern | Count | Details |
|---------|-------|---------|
| HIGH_AMOUNT | 5 | ₹6 lakh ~ ₹20 lakh transactions |
| VELOCITY | 3 | `ACC_FRAUD_VEL` account, consecutive transactions 60 seconds apart |
| GEO_ANOMALY | 2 | `ACC_FRAUD_GEO` account, Delhi → Mumbai in 10 minutes |

> **Note:** When using the `kafka_producer.py --rows` option, this script is called internally  
> but creates a temporary file (`/tmp/tmpXXX.csv`) that is immediately deleted, leaving no file on disk.

---

### 5-2. Send Synthetic Data to Kafka

```bash
source /tmp/sbi-venv/bin/activate
source config/env.conf

# Option A: Send from file (when pre-generated with generate_transactions.py)
python data_gen/kafka_producer.py --input /tmp/txn.csv --rate ${DEMO_RATE}

# Option B: Generate and send on the fly (no file saved)
python data_gen/kafka_producer.py --rows ${DEMO_ROWS} --rate ${DEMO_RATE}
```

**Data generated:**
- Normal transactions (generated by SDV GaussianCopulaSynthesizer)
- HIGH_AMOUNT transactions (₹5 lakh or more)
- VELOCITY transactions (repeated from same account within 5 minutes)
- GEO_ANOMALY transactions (movement over 500 km)

### 5-3. Run Spark Ingest (Kafka → Raw Iceberg)

```bash
bash scripts/02_run_ingest.sh
```

**Schedule via cron (auto-run every minute):**

```bash
crontab -e
# Add:
* * * * * /root/sbi-fraud-detection-pipeline/run_ingest.sh >> /var/log/sbi-ingest.log 2>&1
```

### 5-4. Run Spark ETL (Raw → Curated)

**Usage:**
```
scripts/03_run_etl.sh [YYYY-MM-DD]
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `YYYY-MM-DD` | Optional | yesterday | Partition date to process (`dt=YYYY-MM-DD`) |

**Examples:**
```bash
# Auto-process yesterday (omit parameter)
bash scripts/03_run_etl.sh

# Process a specific date
bash scripts/03_run_etl.sh 2026-06-07

# Reprocess past dates (backfill)
bash scripts/03_run_etl.sh 2026-06-01
bash scripts/03_run_etl.sh 2026-06-02
bash scripts/03_run_etl.sh 2026-06-03
```

> **Note:** If there is no data in `sbi_raw.transactions` for the specified date (`dt`),  
> the message `No data to process for dt=.... Exiting ETL.` will be printed and the job will exit normally.  
> In this case, verify that Steps 5-1 through 5-3 (data generation → Kafka send → ingest) have been completed first.

**Schedule via cron (auto-run daily at 1 AM):**
```bash
crontab -e
# Add:
0 1 * * * /root/sbi-fraud-detection-pipeline/scripts/03_run_etl.sh >> /var/log/sbi-etl.log 2>&1
```

### 5-5. Verify Results

```bash
bash scripts/04_run_report.sh
```

Or run `report/fraud_report.sql` in the Hue (`https://<HS2_HOST>:8889`) SQL Editor.

**Expected results:**
```
fraud_type    | fraud_cnt | total_amount_INR
HIGH_AMOUNT   |     42   |   38,500,000
VELOCITY      |     18   |    9,200,000
GEO_ANOMALY   |      7   |    4,100,000
```

### 5-6. Demo Walkthrough (for customer audience)

```
[1] Cloudera Manager  → verify all services show Green status
[2] SMM               → sbi-fd-transactions-raw message throughput graph
[3] YARN ResourceMgr  → confirm Spark job is running
[4] Hue (Hive)        → observe sbi_raw.transactions row count increasing
[5] Run Spark ETL     → bash scripts/03_run_etl.sh
[6] Hue (Impala)      → query fraud_alerts → view fraud detection results
```

---

## Environment Switching

| File | Target Environment | Purpose |
|------|--------------------|---------|
| `config/env.internal.conf` | Cloudera internal test | Demo development/validation |
| `config/env.uatdev.conf` | SBI UAT / DEV cluster | Customer test environment |
| `config/env.prd.conf` | SBI Production cluster | Live production environment |

```bash
# Switch to UAT/DEV environment
ln -sf config/env.uatdev.conf config/env.conf
vi config/env.uatdev.conf   # fill in CHANGE_ME values

# Switch to Production environment
ln -sf config/env.prd.conf config/env.conf
vi config/env.prd.conf      # fill in CHANGE_ME values

# Return to internal test environment
ln -sf config/env.internal.conf config/env.conf
```

Common steps after switching environments:

```bash
source config/env.conf
bash scripts/01_verify_env.sh
bash infra/01_kafka_setup.sh
bash infra/02_ozone_setup.sh
bash infra/03_iceberg_ddl.sh
```

---

## Kerberos Authentication Overview

All components in this project use the **kinit + OS TGT** method.

```
kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
  ↓
TGT stored in OS Kerberos ticket cache (ccache)
  ↓
Each component references the TGT via GSSAPI for automatic authentication
```

| Component | Authentication method |
|-----------|-----------------------|
| `kafka_producer.py` | Calls `kinit` automatically within the script |
| `scripts/02_run_ingest.sh` | Calls `kinit` automatically after `source config/env.conf` |
| `scripts/03_run_etl.sh` | Same |
| `scripts/04_run_report.sh` | Same |
| Kafka CLI (`infra/*.sh`) | Calls `kinit` automatically within the script |
| Spark (YARN) | Handled by YARN via `--keytab` and `--principal` |
| Hue | Kerberos handled automatically on browser login |

> **TGT validity:** 10 hours by default. No re-authentication needed if the demo completes within 10 hours.

---

## Troubleshooting Guide

### Python Package Installation Failure (Air-gapped)

```
Symptom: No matching distribution found
Cause:   pip download was run on a different OS (e.g. macOS)
Fix:     Re-run pip download on an RHEL 9.6 Bastion machine
```

### Kerberos Authentication Failure

```
Symptom: kinit: Password incorrect  or  Cannot find KDC
Cause:   Keytab file missing or incorrect path
Fix:
  ls -la /opt/cloudera/systest.keytab
  klist -kt /opt/cloudera/systest.keytab
```

### Kafka Connection Failure

```
Symptom: SASL authentication failed
Cause 1: Kerberos TGT expired → source config/env.conf && kinit -kt "${KEYTAB}" "${PRINCIPAL}"
Cause 2: Ranger Kafka policy not applied → check Ranger UI
Cause 3: Incorrect KAFKA_BROKERS hostname → check config/env.conf
```

### Executor Ozone Authentication Error

```
Symptom: Client cannot authenticate via:[TOKEN, KERBEROS]
Cause:   Ozone delegation token not distributed to Spark YARN executors
Fix:     Add the following to CM Safety Valve:
         spark.yarn.access.hadoopFileSystems=ofs://<OZONE_OM_SERVICE_ID>
```

### Iceberg RWSTORAGE Permission Error

```
Symptom: Permission denied: user [systest] does not have [RWSTORAGE] privilege
Cause:   StorageBasedAuthorizationPreEventListener enabled in HMS
Fix:
  1. CM → HMS hive-site.xml Safety Valve
     hive.metastore.pre.event.listeners = (empty value)
  2. Ranger cm_hive → sbi-iceberg-storage-policy
     URL: iceberg://* / Permissions: All (including RW Storage) / User: systest
```

### HMS Connection Drop

```
Symptom: TTransportException: Socket is closed by peer
Cause:   Missing HMS Kerberos configuration
Fix:     Check conf/spark-defaults.conf
         spark.hadoop.hive.metastore.sasl.enabled=true
         spark.hadoop.hive.metastore.kerberos.principal=hive/_HOST@<KRB_REALM>
```

### Ozone Permission Error (hive account)

```
Symptom: User hive doesn't have READ permission to access volume
Cause:   hive account ACL not set on Ozone volume/bucket
Fix:
  sudo -u hdfs ozone sh volume addacl /<VOLUME> --acl "user:hive:rwlc"
  sudo -u hdfs ozone sh bucket addacl /<VOLUME>/sbi-raw --acl "user:hive:rwlc"
  sudo -u hdfs ozone sh bucket addacl /<VOLUME>/sbi-curated --acl "user:hive:rwlc"
```

### Reprocessing from Scratch

```
Symptom: "No new messages, exiting" or data re-ingestion needed
Fix:
  source config/env.conf
  rm -f "${KAFKA_OFFSET_FILE}"
  bash scripts/02_run_ingest.sh
```

---

## Frequently Asked Questions (FAQ)

**Q: I'm new to Cloudera — what does each product do?**

| Product | Simple description | Analogy |
|---------|-------------------|---------|
| Kafka | A queue that temporarily holds data | Mailbox |
| Spark | An engine that processes data quickly | Data analyst |
| Ozone | Distributed storage for large files | Warehouse |
| Iceberg | Manages data in table format | Organized filing cabinet |
| Hive/Hue | Query stored data with SQL | Library catalog |
| Ranger | Controls who can access what | Security guard |

**Q: Why use kafka-python instead of confluent-kafka?**

`confluent-kafka` internally requires a C library (`librdkafka`).  
In an air-gapped RHEL environment, building the C library can be difficult and installation may fail.  
`kafka-python` is pure Python and installs reliably via `pip download` → `--no-index`.

**Q: Why use Batch instead of Spark Streaming?**

A 1-minute batch interval is far simpler to operate than Spark Streaming.  
It prevents duplicates using an offset file (no checkpoint files), and can be scheduled via cron.  
It is well-suited to fraud detection scenarios where 1-minute latency is acceptable.

**Q: Is the demo data real transaction data?**

No, it is synthetic data generated by the SDV (Synthetic Data Vault) library.  
The statistical distribution resembles real data, but it contains no actual customer information.

**Q: How do I clean up data after the demo?**

```bash
source config/env.conf

# Reset Kafka topic messages (auto-deleted based on retention policy)
# Delete Iceberg table data (run in Hue)
# TRUNCATE TABLE sbi_raw.transactions;
# TRUNCATE TABLE sbi_curated.fraud_alerts;

# Delete offset file
rm -f "${KAFKA_OFFSET_FILE}"
```

---

## Technical Stack Details

| Item | Value |
|------|-------|
| CDP version | 7.3.1 |
| Spark version | 3.5.x (Scala 2.12) |
| Iceberg version | 1.5.2 |
| Ozone version | 1.4.0 (bundled with CDP) |
| Python | **3.9.x** (built into RHEL 9.6) |
| Kafka library | kafka-python 2.0+ (pure Python, air-gapped compatible) |
| SDV | 1.9.0+ (GaussianCopulaSynthesizer) |
| Security | Kerberos + Auto-TLS + Ranger (all enabled) |
| Kerberos method | kinit + OS TGT (GSSAPI) — same across all components |
| Run as | systest (single account) |
| Keytab path | /opt/cloudera/systest.keytab |
| Kafka port | 9093 (SASL_SSL) |
| HiveServer2 port | 10000 |
| Ozone OM port | 9862 |

---

*This demo is part of the Cloudera SBI Fraud Detection PoC project.*  
*Contact: Cloudera Solutions Engineering Team*
