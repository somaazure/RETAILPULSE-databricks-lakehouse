# Databricks notebook source
"""Generate a single batch of RetailPulse order CSV files for one-shot job runs."""

from __future__ import annotations

import csv
import random
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS retailpulse.bronze.orders_files")

OUTPUT_DIR = Path("/Volumes/retailpulse/bronze/orders_files/orders_business_ts/")
LOCAL_STAGING_DIR = Path("/local_disk0/tmp/retailpulse/orders/")
VALID_RECORDS_PER_FILE = 250
MAX_PAST_DAYS = 90


def random_order_timestamp() -> str:
    now_utc = datetime.now(timezone.utc)
    offset = timedelta(
        days=random.randint(0, MAX_PAST_DAYS),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return (now_utc - offset).strftime("%Y-%m-%d %H:%M:%S")


def generate_valid_order_row() -> dict[str, int | float | str]:
    quantity = random.randint(1, 5)
    price = round(random.uniform(5.0, 500.0), 2)
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "product_id": random.randint(100, 999),
        "quantity": quantity,
        "price": price,
        "order_timestamp": random_order_timestamp(),
    }


def write_batch(valid_batch_size: int = VALID_RECORDS_PER_FILE) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_STAGING_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"orders_{timestamp}.csv"
    local_file_path = LOCAL_STAGING_DIR / file_name
    output_file_path = OUTPUT_DIR / file_name

    rows = [generate_valid_order_row() for _ in range(valid_batch_size)]

    with local_file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["order_id", "customer_id", "product_id", "quantity", "price", "order_timestamp"],
        )
        writer.writeheader()
        writer.writerows(rows)

    shutil.copyfile(local_file_path, output_file_path)
    return output_file_path


if __name__ == "__main__":
    output_file = write_batch()
    print(f"Created one-shot sample order batch: {output_file}")
    print(f"Valid records: {VALID_RECORDS_PER_FILE}")
    print(f"Randomized order_timestamp range: last {MAX_PAST_DAYS} days")
