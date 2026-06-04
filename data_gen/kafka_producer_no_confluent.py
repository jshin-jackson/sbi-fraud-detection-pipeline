# -*- coding: utf-8 -*-
"""
Kerberos(SASL_SSL) 인증을 사용하여 합성 거래 데이터를 Kafka 토픽으로 전송합니다.

이 버전은 Python confluent-kafka/librdkafka를 사용하지 않습니다.
대신 Cloudera/Kafka에 포함된 Java kafka-console-producer를 subprocess로 호출합니다.
Kerberos(GSSAPI)는 Java Kafka client가 처리하므로, librdkafka GSSAPI build 이슈를 피할 수 있습니다.

사용법:
    # CSV 파일로부터 전송
    python3 kafka_producer_no_confluent.py --input /tmp/transactions.csv --rate 100

    # SDV 생성 후 바로 스트리밍 (--rows 옵션 사용)
    python3 kafka_producer_no_confluent.py --rows 5000 --rate 50

환경변수:
    KAFKA_BROKERS          Kafka 브로커 주소
    KAFKA_TOPIC            대상 토픽
    KAFKA_KEYTAB           Kerberos keytab 경로
    KAFKA_PRINCIPAL        Kerberos principal
    KAFKA_TRUSTSTORE       SSL truststore JKS 경로
    KAFKA_TRUSTSTORE_PW    SSL truststore password
    KAFKA_PRODUCER_CMD     kafka-console-producer 실행 파일 경로 또는 명령어
                           기본값: kafka-console-producer

주의:
    이 스크립트는 실행 시 kinit -kt 를 먼저 수행합니다.
    Kerberos ticket cache가 정상이어야 Java Kafka client가 SASL/GSSAPI 인증을 수행할 수 있습니다.
"""

from __future__ import print_function

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

import pandas as pd


# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------

KAFKA_BROKERS = os.environ.get(
    "KAFKA_BROKERS",
    "ccycloud-1.jshin.root.comops.site:9093,"
    "ccycloud-2.jshin.root.comops.site:9093,"
    "ccycloud-3.jshin.root.comops.site:9093",
)
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sbi-transactions-raw")
KAFKA_KEYTAB = os.environ.get("KAFKA_KEYTAB", "/root/systest.keytab")
KAFKA_PRINCIPAL = os.environ.get("KAFKA_PRINCIPAL", "systest@ROOT.COMOPS.SITE")
KAFKA_TRUSTSTORE = os.environ.get("KAFKA_TRUSTSTORE", "/etc/security/certs/truststore.jks")
KAFKA_TRUSTSTORE_PW = os.environ.get("KAFKA_TRUSTSTORE_PW", "changeit")
KAFKA_PRODUCER_CMD = os.environ.get("KAFKA_PRODUCER_CMD", "kafka-console-producer")


def run_cmd(cmd, check=True, capture_output=False):
    """Python 3.6+ compatible subprocess wrapper."""
    kwargs = {"check": check}
    if capture_output:
        kwargs.update({"stdout": subprocess.PIPE, "stderr": subprocess.PIPE})
    return subprocess.run(cmd, **kwargs)


def ensure_command_exists(command):
    """명령어가 PATH에 있는지 확인합니다. 절대경로면 파일 존재 여부를 확인합니다."""
    if os.path.isabs(command):
        if not os.path.exists(command):
            raise RuntimeError("Kafka producer command not found: {0}".format(command))
        return command

    found = shutil.which(command)
    if not found:
        raise RuntimeError(
            "Kafka producer command not found in PATH: {0}\n"
            "Set KAFKA_PRODUCER_CMD to the full path of kafka-console-producer."
            .format(command)
        )
    return found


def kinit_with_keytab():
    """keytab으로 Kerberos ticket을 발급합니다."""
    if not os.path.exists(KAFKA_KEYTAB):
        raise RuntimeError("Keytab not found: {0}".format(KAFKA_KEYTAB))

    print("Kerberos kinit 실행: {0}".format(KAFKA_PRINCIPAL))
    run_cmd(["kinit", "-kt", KAFKA_KEYTAB, KAFKA_PRINCIPAL], check=True)

    # klist는 실패해도 producer 실행을 막지는 않도록 출력 확인용으로만 수행
    try:
        run_cmd(["klist"], check=False)
    except FileNotFoundError:
        pass


def build_producer_config_file():
    """Java Kafka console producer용 properties 파일을 생성합니다."""
    if not os.path.exists(KAFKA_TRUSTSTORE):
        raise RuntimeError("Truststore not found: {0}".format(KAFKA_TRUSTSTORE))

    properties = """
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
ssl.truststore.location={truststore}
ssl.truststore.password={truststore_pw}
acks=all
retries=3
linger.ms=10
batch.size=65536
compression.type=snappy
client.id=sbi-fraud-producer
""".strip().format(
        truststore=KAFKA_TRUSTSTORE,
        truststore_pw=KAFKA_TRUSTSTORE_PW,
    )

    fd, path = tempfile.mkstemp(prefix="sbi-kafka-producer-", suffix=".properties")
    with os.fdopen(fd, "w") as f:
        f.write(properties)
        f.write("\n")

    os.chmod(path, 0o600)
    return path


def row_to_json(row):
    """DataFrame row를 JSON 문자열로 변환합니다."""
    record = {}
    for key, value in row.items():
        if hasattr(value, "isoformat"):
            record[key] = value.isoformat()
        else:
            # pandas NaN을 JSON null로 처리
            try:
                if pd.isna(value):
                    record[key] = None
                else:
                    record[key] = value
            except TypeError:
                record[key] = value

    if "is_fraud" in record and record["is_fraud"] is not None:
        record["is_fraud"] = bool(record["is_fraud"])

    return json.dumps(record, ensure_ascii=False, default=str)


def start_console_producer(config_file):
    """kafka-console-producer 프로세스를 시작합니다."""
    producer_cmd = ensure_command_exists(KAFKA_PRODUCER_CMD)
    cmd = [
        producer_cmd,
        "--broker-list", KAFKA_BROKERS,
        "--topic", KAFKA_TOPIC,
        "--producer.config", config_file,
    ]

    print("Kafka producer command: {0}".format(" ".join(cmd)))

    return subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1,
    )


def produce_from_dataframe(df, rate):
    """
    DataFrame을 지정한 rate(건/초)로 Kafka에 전송합니다.

    Args:
        df:   전송할 거래 데이터프레임
        rate: 초당 전송 건수 (0이면 최대 속도)
    """
    kinit_with_keytab()
    config_file = build_producer_config_file()

    sleep_interval = 1.0 / rate if rate > 0 else 0
    total = len(df)
    sent = 0

    print("Kafka 브로커: {0}".format(KAFKA_BROKERS))
    print("토픽: {0}".format(KAFKA_TOPIC))
    print("총 {0}건을 {1}건/초 속도로 전송 시작...".format(total, rate))

    process = None
    try:
        process = start_console_producer(config_file)

        for _, row in df.iterrows():
            value = row_to_json(row.to_dict())
            process.stdin.write(value + "\n")
            sent += 1

            if sent % 500 == 0:
                process.stdin.flush()
                print("  전송: {0}/{1}".format(sent, total))

                # producer가 이미 종료되었는지 확인
                rc = process.poll()
                if rc is not None:
                    stderr = process.stderr.read() if process.stderr else ""
                    raise RuntimeError(
                        "kafka-console-producer exited early with rc={0}\n{1}".format(rc, stderr)
                    )

            if sleep_interval > 0:
                time.sleep(sleep_interval)

        process.stdin.flush()
        process.stdin.close()

        rc = process.wait(timeout=60)
        stderr = process.stderr.read() if process.stderr else ""
        if rc != 0:
            raise RuntimeError(
                "kafka-console-producer failed with rc={0}\n{1}".format(rc, stderr)
            )

        print("전송 완료: {0}건".format(sent))

    except KeyboardInterrupt:
        print("\n중단됨. {0}건 전송 시도 완료.".format(sent))
        if process and process.stdin:
            try:
                process.stdin.close()
            except Exception:
                pass
        if process:
            process.terminate()
    finally:
        try:
            os.unlink(config_file)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="SBI 거래 데이터 Kafka Producer - Java Kafka client wrapper")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=str, help="CSV 입력 파일 경로")
    source.add_argument("--rows", type=int, help="SDV로 즉시 생성할 건수")

    parser.add_argument("--rate", type=int, default=100,
                        help="초당 전송 건수 (기본: 100, 0=최대 속도)")
    parser.add_argument("--topic", type=str, default=None,
                        help="Kafka 토픽명 (기본: 환경변수 KAFKA_TOPIC)")
    args = parser.parse_args()

    global KAFKA_TOPIC
    if args.topic:
        KAFKA_TOPIC = args.topic

    if args.input:
        df = pd.read_csv(args.input)
        print("파일 로드 완료: {0} ({1}건)".format(args.input, len(df)))
    else:
        sys.path.insert(0, os.path.dirname(__file__))
        from generate_transactions import generate

        tmp_path = tempfile.mktemp(suffix=".csv")
        generate(args.rows, tmp_path, "csv")
        df = pd.read_csv(tmp_path)
        os.unlink(tmp_path)

    produce_from_dataframe(df, args.rate)


if __name__ == "__main__":
    main()
