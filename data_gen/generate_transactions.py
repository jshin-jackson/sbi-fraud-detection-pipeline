"""
SDV(Synthetic Data Vault)를 사용하여 SBI 은행 합성 거래 데이터를 생성합니다.

사용법:
    python generate_transactions.py --rows 10000 --output transactions.csv
    python generate_transactions.py --rows 5000 --output transactions.json --format json
"""

import argparse
import json
import uuid
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer


CHANNELS = ["ONLINE", "ATM", "POS"]
MERCHANT_CATS = [
    "GROCERY", "FUEL", "RESTAURANT", "TRAVEL", "ELECTRONICS",
    "PHARMACY", "JEWELLERY", "TRANSFER", "ATM_WITHDRAWAL", "ECOMMERCE"
]

# 인도 주요 도시 위경도 (사기 위치 이상 탐지 시나리오용)
CITY_COORDS = [
    (28.6139, 77.2090),   # New Delhi
    (19.0760, 72.8777),   # Mumbai
    (12.9716, 77.5946),   # Bangalore
    (22.5726, 88.3639),   # Kolkata
    (13.0827, 80.2707),   # Chennai
    (17.3850, 78.4867),   # Hyderabad
    (23.0225, 72.5714),   # Ahmedabad
    (18.5204, 73.8567),   # Pune
]


def build_seed_dataframe(n: int = 2000) -> pd.DataFrame:
    """SDV 학습용 시드 데이터프레임을 생성합니다."""
    random.seed(42)
    np.random.seed(42)

    base_time = datetime(2024, 1, 1)
    records = []

    for _ in range(n):
        is_fraud = random.random() < 0.05  # 5% 사기 비율
        city = random.choice(CITY_COORDS)
        lat_jitter = np.random.uniform(-0.5, 0.5)
        lon_jitter = np.random.uniform(-0.5, 0.5)

        if is_fraud:
            amount = round(random.uniform(300000, 2000000), 2)
        else:
            amount = round(random.uniform(100, 50000), 2)

        offset_seconds = random.randint(0, 60 * 24 * 365)
        ts = base_time + timedelta(seconds=offset_seconds)

        records.append({
            "account_id": f"ACC{random.randint(10000, 99999)}",
            "timestamp": ts.isoformat(),
            "amount": amount,
            "merchant_id": f"MER{random.randint(1000, 9999)}",
            "merchant_cat": random.choice(MERCHANT_CATS),
            "location_lat": round(city[0] + lat_jitter, 6),
            "location_lon": round(city[1] + lon_jitter, 6),
            "channel": random.choice(CHANNELS),
            "is_fraud": is_fraud,
        })

    return pd.DataFrame(records)


def train_synthesizer(seed_df: pd.DataFrame) -> GaussianCopulaSynthesizer:
    """SDV GaussianCopula 합성기를 학습합니다."""
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(seed_df)

    # 타입 수동 보정
    metadata.update_column("account_id", sdtype="id")
    metadata.update_column("merchant_id", sdtype="id")
    metadata.update_column("timestamp", sdtype="datetime", datetime_format="%Y-%m-%dT%H:%M:%S")
    metadata.update_column("channel", sdtype="categorical")
    metadata.update_column("merchant_cat", sdtype="categorical")
    metadata.update_column("is_fraud", sdtype="boolean")

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(seed_df)
    return synthesizer


def add_transaction_ids(df: pd.DataFrame) -> pd.DataFrame:
    """transaction_id(UUID) 컬럼을 추가합니다."""
    df = df.copy()
    df.insert(0, "transaction_id", [str(uuid.uuid4()) for _ in range(len(df))])
    return df


def inject_fraud_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """
    데모 시나리오용 명시적 사기 패턴을 주입합니다.
    - HIGH_AMOUNT: 500,000 INR 초과 거래
    - VELOCITY:    동일 계좌 5분 내 연속 거래
    - GEO_ANOMALY: 물리적으로 불가능한 위치 이동
    """
    df = df.copy()

    # HIGH_AMOUNT 사기 5건
    idx_high = df.sample(5, random_state=1).index
    df.loc[idx_high, "amount"] = [600000, 750000, 1200000, 900000, 2000000]
    df.loc[idx_high, "is_fraud"] = True

    # VELOCITY 사기: 동일 계좌에 60초 간격 거래 3건
    velocity_account = "ACC_FRAUD_VEL"
    base_ts = datetime(2024, 6, 15, 14, 0, 0)
    velocity_rows = []
    for i in range(3):
        velocity_rows.append({
            "transaction_id": str(uuid.uuid4()),
            "account_id": velocity_account,
            "timestamp": (base_ts + timedelta(seconds=i * 60)).isoformat(),
            "amount": round(random.uniform(10000, 30000), 2),
            "merchant_id": f"MER{7000 + i}",
            "merchant_cat": "ECOMMERCE",
            "location_lat": 28.6139,
            "location_lon": 77.2090,
            "channel": "ONLINE",
            "is_fraud": True,
        })
    velocity_df = pd.DataFrame(velocity_rows)
    df = pd.concat([df, velocity_df], ignore_index=True)

    # GEO_ANOMALY 사기: 10분 간격 Delhi → Mumbai (1400 km)
    geo_account = "ACC_FRAUD_GEO"
    geo_rows = [
        {
            "transaction_id": str(uuid.uuid4()),
            "account_id": geo_account,
            "timestamp": datetime(2024, 7, 1, 10, 0, 0).isoformat(),
            "amount": 15000.0,
            "merchant_id": "MER8001",
            "merchant_cat": "RESTAURANT",
            "location_lat": 28.6139, "location_lon": 77.2090,  # Delhi
            "channel": "POS",
            "is_fraud": False,
        },
        {
            "transaction_id": str(uuid.uuid4()),
            "account_id": geo_account,
            "timestamp": datetime(2024, 7, 1, 10, 10, 0).isoformat(),
            "amount": 25000.0,
            "merchant_id": "MER8002",
            "merchant_cat": "JEWELLERY",
            "location_lat": 19.0760, "location_lon": 72.8777,  # Mumbai
            "channel": "POS",
            "is_fraud": True,
        },
    ]
    geo_df = pd.DataFrame(geo_rows)
    df = pd.concat([df, geo_df], ignore_index=True)

    return df.reset_index(drop=True)


def generate(n_rows: int, output_path: str, fmt: str = "csv") -> None:
    print(f"[1/3] 시드 데이터 생성 중...")
    seed_df = build_seed_dataframe(2000)

    print(f"[2/3] SDV 합성기 학습 중...")
    synthesizer = train_synthesizer(seed_df)

    print(f"[3/3] {n_rows}건 합성 데이터 생성 중...")
    synthetic_df = synthesizer.sample(num_rows=n_rows)
    synthetic_df = add_transaction_ids(synthetic_df)
    synthetic_df = inject_fraud_patterns(synthetic_df)

    fraud_count = synthetic_df["is_fraud"].sum()
    print(f"    총 {len(synthetic_df)}건 생성 완료 (사기: {fraud_count}건, "
          f"{100 * fraud_count / len(synthetic_df):.1f}%)")

    if fmt == "json":
        records = synthetic_df.to_dict(orient="records")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, default=str, indent=2)
    else:
        synthetic_df.to_csv(output_path, index=False)

    print(f"저장 완료: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SDV 합성 거래 데이터 생성기")
    parser.add_argument("--rows", type=int, default=10000, help="생성할 거래 건수 (기본: 10000)")
    parser.add_argument("--output", type=str, default="transactions.csv", help="출력 파일 경로")
    parser.add_argument("--format", dest="fmt", choices=["csv", "json"], default="csv")
    args = parser.parse_args()

    generate(args.rows, args.output, args.fmt)
