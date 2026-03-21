"""Generate synthetic RetailPulse order CSV files on a fixed interval.

This script is intended for use in Databricks. It continuously writes small
CSV batches to a Unity Catalog volume so downstream ingestion notebooks can
pick them up.
"""

from __future__ import annotations

import csv
import random
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path("/Volumes/retailpulse/bronze/orders_files/orders/")
LOCAL_STAGING_DIR = Path("/local_disk0/tmp/retailpulse/orders/")
WRITE_INTERVAL_SECONDS = 10
RECORDS_PER_FILE = 25


def generate_order_row() -> dict[str, int | float | str]:
    quantity = random.randint(1, 5)
    price = round(random.uniform(5.0, 500.0), 2)
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "product_id": random.randint(100, 999),
        "quantity": quantity,
        "price": price,
    }


def write_batch(batch_size: int = RECORDS_PER_FILE) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_STAGING_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"orders_{timestamp}.csv"
    local_file_path = LOCAL_STAGING_DIR / file_name
    output_file_path = OUTPUT_DIR / file_name

    with local_file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["order_id", "customer_id", "product_id", "quantity", "price"],
        )
        writer.writeheader()
        writer.writerows(generate_order_row() for _ in range(batch_size))

    shutil.copyfile(local_file_path, output_file_path)
    return output_file_path


if __name__ == "__main__":
    print(f"Writing synthetic order files to {OUTPUT_DIR}")
    while True:
        output_file = write_batch()
        print(f"Created {output_file}")
        time.sleep(WRITE_INTERVAL_SECONDS)
