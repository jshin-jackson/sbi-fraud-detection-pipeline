"""
Sends synthetic transaction data to a Kafka topic using Kerberos (SASL_SSL) authentication.

Usage:
    # Send from a CSV file
    python kafka_producer.py --input transactions.csv --rate 100

    # Generate via SDV and stream immediately (using --rows option)
    python kafka_producer.py --rows 5000 --rate 50

Environment variables:
    KAFKA_BROKERS       Kafka broker addresses (default: ccycloud-1~3.jshin.root.comops.site:9093)
    KAFKA_TOPIC         Target topic (default: sbi-fd-transactions-raw)
    KAFKA_KEYTAB        Kerberos keytab path
    KAFKA_PRINCIPAL     Kerberos principal (e.g. systest@ROOT.COMOPS.SITE)
    KAFKA_CA_PEM        SSL CA certificate PEM file path
"""

import argparse
import json
import os
import ssl
import tempfile
import time
import sys
import subprocess
import pandas as pd

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sbi-fd-transactions-raw")
KAFKA_KEYTAB = os.environ.get("KAFKA_KEYTAB", os.environ.get("KEYTAB",    ""))
KAFKA_PRINCIPAL = os.environ.get("KAFKA_PRINCIPAL", os.environ.get("PRINCIPAL", ""))
KAFKA_CA_PEM = os.environ.get("KAFKA_CA_PEM", "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem")


def kinit() -> None:
    """
    Refreshes the Kerberos TGT.
    kafka-python uses the OS-level Kerberos ticket cache,
    so a prior kinit with the keytab is required.
    """
    if not os.path.exists(KAFKA_KEYTAB):
        print("[WARNING] Keytab file not found, skipping kinit.")
        return
    try:
        subprocess.run(
            ["kinit", "-kt", KAFKA_KEYTAB, KAFKA_PRINCIPAL],
            check=True,
            capture_output=True,
        )
        print(f"[INFO] kinit succeeded: {KAFKA_PRINCIPAL}")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"[WARNING] kinit failed (using existing ticket): {e}", file=sys.stderr)


def build_producer() -> KafkaProducer:
    """Creates and returns a Kerberos SASL_SSL KafkaProducer."""
    kinit()

    ssl_context = ssl.create_default_context()
    if os.path.exists(KAFKA_CA_PEM):
        ssl_context.load_verify_locations(cafile=KAFKA_CA_PEM)
        print(f"[INFO] SSL CA certificate loaded: {KAFKA_CA_PEM}")
    else:
        print(f"[WARNING] CA PEM file not found ({KAFKA_CA_PEM}), SSL verification disabled.", file=sys.stderr)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    bootstrap_servers = KAFKA_BROKERS.split(",")

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="GSSAPI",
        sasl_kerberos_service_name="kafka",
        ssl_context=ssl_context,
        client_id="sbi-fraud-producer",
        acks="all",
        retries=3,
        linger_ms=10,
        batch_size=65536,
        compression_type="snappy",
    )


def row_to_json(row: dict) -> bytes:
    """Converts a DataFrame row to JSON bytes."""
    record = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}
    if "is_fraud" in record:
        record["is_fraud"] = bool(record["is_fraud"])
    return json.dumps(record, ensure_ascii=False, default=str).encode("utf-8")


def produce_from_dataframe(df: pd.DataFrame, rate: int) -> None:
    """
    Sends a DataFrame to Kafka at the specified rate (records/second).

    Args:
        df:   Transaction DataFrame to send
        rate: Records per second (0 for maximum speed)
    """
    producer = build_producer()

    sleep_interval = 1.0 / rate if rate > 0 else 0
    total = len(df)
    sent = 0

    print(f"Kafka brokers: {KAFKA_BROKERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Sending {total} records at {rate} records/sec...")

    def _on_error(e: KafkaError) -> None:
        print(f"[ERROR] Send failed: {e}", file=sys.stderr)

    try:
        for _, row in df.iterrows():
            key = str(row.get("account_id", "unknown")).encode("utf-8")
            value = row_to_json(row.to_dict())

            producer.send(KAFKA_TOPIC, key=key, value=value).add_errback(_on_error)

            sent += 1
            if sent % 500 == 0:
                print(f"  Sent: {sent}/{total}")

            if sleep_interval > 0:
                time.sleep(sleep_interval)

        producer.flush(timeout=30)
        print(f"Send complete: {sent} records")

    except KafkaError as e:
        print(f"[ERROR] Kafka send error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\nInterrupted. {sent} records sent.")
        producer.flush(timeout=10)
    finally:
        producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="SBI transaction data Kafka Producer")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=str, help="CSV input file path")
    source.add_argument("--rows", type=int, help="Number of records to generate with SDV")

    parser.add_argument("--rate", type=int, default=100,
                        help="Records per second (default: 100, 0=maximum speed)")
    parser.add_argument("--topic", type=str, default=None,
                        help="Kafka topic name (default: KAFKA_TOPIC environment variable)")
    args = parser.parse_args()

    global KAFKA_TOPIC
    if args.topic:
        KAFKA_TOPIC = args.topic

    if args.input:
        df = pd.read_csv(args.input)
        print(f"File loaded: {args.input} ({len(df)} records)")
    else:
        # Import SDV generation module from the same directory
        sys.path.insert(0, os.path.dirname(__file__))
        from generate_transactions import generate

        tmp_path = tempfile.mktemp(suffix=".csv")
        generate(args.rows, tmp_path, "csv")
        df = pd.read_csv(tmp_path)
        os.unlink(tmp_path)

    produce_from_dataframe(df, args.rate)


if __name__ == "__main__":
    main()
