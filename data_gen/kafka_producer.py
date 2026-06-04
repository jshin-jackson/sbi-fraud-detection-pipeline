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
    KAFKA_CA_PEM        SSL CA 인증서 PEM 파일 경로
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
# 설정
# ---------------------------------------------------------------------------

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "ccycloud-1.jshin.root.comops.site:9093,ccycloud-2.jshin.root.comops.site:9093,ccycloud-3.jshin.root.comops.site:9093")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sbi-transactions-raw")
KAFKA_KEYTAB = os.environ.get("KAFKA_KEYTAB", "/root/systest.keytab")
KAFKA_PRINCIPAL = os.environ.get("KAFKA_PRINCIPAL", "systest@ROOT.COMOPS.SITE")
KAFKA_CA_PEM = os.environ.get("KAFKA_CA_PEM", "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem")


def kinit() -> None:
    """
    Kerberos TGT를 갱신합니다.
    kafka-python은 OS 수준 Kerberos 티켓 캐시를 사용하므로
    keytab으로 사전 kinit이 필요합니다.
    """
    if not os.path.exists(KAFKA_KEYTAB):
        print("[경고] keytab 파일 없음, kinit 생략합니다.")
        return
    try:
        subprocess.run(
            ["kinit", "-kt", KAFKA_KEYTAB, KAFKA_PRINCIPAL],
            check=True,
            capture_output=True,
        )
        print(f"[정보] kinit 성공: {KAFKA_PRINCIPAL}")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"[경고] kinit 실패 (기존 티켓 사용): {e}", file=sys.stderr)


def build_producer() -> KafkaProducer:
    """Kerberos SASL_SSL KafkaProducer를 생성하여 반환합니다."""
    kinit()

    ssl_context = ssl.create_default_context()
    if os.path.exists(KAFKA_CA_PEM):
        ssl_context.load_verify_locations(cafile=KAFKA_CA_PEM)
        print(f"[정보] SSL CA 인증서 로드: {KAFKA_CA_PEM}")
    else:
        print(f"[경고] CA PEM 파일 없음({KAFKA_CA_PEM}), SSL 검증 비활성화됩니다.", file=sys.stderr)
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
    """DataFrame row를 JSON bytes로 변환합니다."""
    record = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}
    if "is_fraud" in record:
        record["is_fraud"] = bool(record["is_fraud"])
    return json.dumps(record, ensure_ascii=False, default=str).encode("utf-8")


def produce_from_dataframe(df: pd.DataFrame, rate: int) -> None:
    """
    DataFrame을 지정한 rate(건/초)로 Kafka에 전송합니다.

    Args:
        df:   전송할 거래 데이터프레임
        rate: 초당 전송 건수 (0이면 최대 속도)
    """
    producer = build_producer()

    sleep_interval = 1.0 / rate if rate > 0 else 0
    total = len(df)
    sent = 0

    print(f"Kafka 브로커: {KAFKA_BROKERS}")
    print(f"토픽: {KAFKA_TOPIC}")
    print(f"총 {total}건을 {rate}건/초 속도로 전송 시작...")

    def _on_error(e: KafkaError) -> None:
        print(f"[오류] 전송 실패: {e}", file=sys.stderr)

    try:
        for _, row in df.iterrows():
            key = str(row.get("account_id", "unknown")).encode("utf-8")
            value = row_to_json(row.to_dict())

            producer.send(KAFKA_TOPIC, key=key, value=value).add_errback(_on_error)

            sent += 1
            if sent % 500 == 0:
                print(f"  전송: {sent}/{total}")

            if sleep_interval > 0:
                time.sleep(sleep_interval)

        producer.flush(timeout=30)
        print(f"전송 완료: {sent}건")

    except KafkaError as e:
        print(f"[오류] Kafka 전송 중 오류 발생: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n중단됨. {sent}건 전송 완료.")
        producer.flush(timeout=10)
    finally:
        producer.close()


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
