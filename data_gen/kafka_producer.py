"""
Kerberos(SASL_SSL) 인증을 사용하여 합성 거래 데이터를 Kafka 토픽으로 전송합니다.

사용법:
    # CSV 파일로부터 전송
    python kafka_producer.py --input transactions.csv --rate 100

    # SDV 생성 후 바로 스트리밍 (--rows 옵션 사용)
    python kafka_producer.py --rows 5000 --rate 50

환경변수:
    KAFKA_BROKERS       Kafka 브로커 주소 (기본: ccycloud-1~3.jshin.root.comops.site:9093)
    KAFKA_TOPIC         대상 토픽 (기본: sbi-transactions-raw)
    KAFKA_KEYTAB        Kerberos keytab 경로
    KAFKA_PRINCIPAL     Kerberos 주체 (예: systest@ROOT.COMOPS.SITE)
    KAFKA_TRUSTSTORE    SSL truststore JKS 경로
    KAFKA_TRUSTSTORE_PW SSL truststore 패스워드
"""

import argparse
import json
import os
import tempfile
import time
import sys
import subprocess
import pandas as pd

from confluent_kafka import Producer, KafkaException


# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "ccycloud-1.jshin.root.comops.site:9093,ccycloud-2.jshin.root.comops.site:9093,ccycloud-3.jshin.root.comops.site:9093")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sbi-transactions-raw")
KAFKA_KEYTAB = os.environ.get("KAFKA_KEYTAB", "/root/systest.keytab")
KAFKA_PRINCIPAL = os.environ.get("KAFKA_PRINCIPAL", "systest@ROOT.COMOPS.SITE")
KAFKA_TRUSTSTORE = os.environ.get("KAFKA_TRUSTSTORE", "/etc/security/certs/truststore.jks")
KAFKA_TRUSTSTORE_PW = os.environ.get("KAFKA_TRUSTSTORE_PW", "changeit")


def build_kafka_config() -> dict:
    """Kerberos SASL_SSL Kafka 설정을 반환합니다."""
    config = {
        "bootstrap.servers": KAFKA_BROKERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "GSSAPI",
        "sasl.kerberos.service.name": "kafka",
        "sasl.kerberos.keytab": KAFKA_KEYTAB,
        "sasl.kerberos.principal": KAFKA_PRINCIPAL,
        "ssl.ca.location": _export_truststore_pem(),
        "client.id": "sbi-fraud-producer",
        "acks": "all",
        "retries": 3,
        "linger.ms": 10,
        "batch.size": 65536,
        "compression.type": "snappy",
    }
    # ssl.ca.location이 비어 있으면(PEM 변환 실패) SSL 인증서 검증 비활성화
    if not config["ssl.ca.location"]:
        config.pop("ssl.ca.location")
        config["enable.ssl.certificate.verification"] = False
    return config


def _export_truststore_pem() -> str:
    """
    JKS truststore에서 PEM 인증서를 추출합니다.
    confluent-kafka는 PEM 형식을 사용하므로 변환이 필요합니다.
    """
    pem_path = "/tmp/sbi-kafka-ca.pem"
    if os.path.exists(pem_path):
        return pem_path
    try:
        subprocess.run(
            [
                "keytool", "-exportcert",
                "-keystore", KAFKA_TRUSTSTORE,
                "-storepass", KAFKA_TRUSTSTORE_PW,
                "-alias", "CARoot",
                "-rfc",
                "-file", pem_path,
            ],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        # keytool 미설치 또는 인증서 변환 실패 시 빈 경로 반환 (개발 환경)
        print("[경고] truststore PEM 변환 실패, SSL 검증 비활성화됩니다.")
        return ""
    return pem_path


def delivery_report(err, msg) -> None:
    if err:
        print(f"[오류] 전송 실패: {err}", file=sys.stderr)


def row_to_json(row: dict) -> str:
    """DataFrame row를 JSON 문자열로 변환합니다."""
    record = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}
    # numpy bool → Python bool 변환
    if "is_fraud" in record:
        record["is_fraud"] = bool(record["is_fraud"])
    return json.dumps(record, ensure_ascii=False, default=str)


def produce_from_dataframe(df: pd.DataFrame, rate: int) -> None:
    """
    DataFrame을 지정한 rate(건/초)로 Kafka에 전송합니다.

    Args:
        df:   전송할 거래 데이터프레임
        rate: 초당 전송 건수 (0이면 최대 속도)
    """
    config = build_kafka_config()
    producer = Producer(config)

    sleep_interval = 1.0 / rate if rate > 0 else 0
    total = len(df)
    sent = 0

    print(f"Kafka 브로커: {KAFKA_BROKERS}")
    print(f"토픽: {KAFKA_TOPIC}")
    print(f"총 {total}건을 {rate}건/초 속도로 전송 시작...")

    try:
        for _, row in df.iterrows():
            key = row.get("account_id", "unknown")
            value = row_to_json(row.to_dict())

            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(key).encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=delivery_report,
            )

            sent += 1
            if sent % 500 == 0:
                producer.poll(0)
                print(f"  전송: {sent}/{total}")

            if sleep_interval > 0:
                time.sleep(sleep_interval)

        producer.flush(timeout=30)
        print(f"전송 완료: {sent}건")

    except KafkaException as e:
        print(f"[오류] Kafka 전송 중 오류 발생: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n중단됨. {sent}건 전송 완료.")
        producer.flush(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser(description="SBI 거래 데이터 Kafka Producer")
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
        print(f"파일 로드 완료: {args.input} ({len(df)}건)")
    else:
        # SDV 생성 모듈 임포트 (동일 디렉터리)
        sys.path.insert(0, os.path.dirname(__file__))
        from generate_transactions import generate

        tmp_path = tempfile.mktemp(suffix=".csv")
        generate(args.rows, tmp_path, "csv")
        df = pd.read_csv(tmp_path)
        os.unlink(tmp_path)

    produce_from_dataframe(df, args.rate)


if __name__ == "__main__":
    main()
