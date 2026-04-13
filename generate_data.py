"""
generate_data.py
----------------
Simulates a daily raw event file, as if it were dumped by an upstream system.
Each run produces one CSV uploaded to S3 under the raw/ prefix.

Intentionally injects bad rows so the pipeline has something real to catch.

Bad row types we simulate:
  - missing_user_id    : user_id is blank
  - negative_amount    : amount is a negative number
  - future_timestamp   : timestamp is set ahead of today
  - invalid_status     : status is not one of the allowed values
  - duplicate_event_id : same event_id appears more than once
"""

import csv
import io
import os
import random
import boto3
from datetime import datetime, timedelta

# ── Constants ────────────────────────────────────────────────────────────────

S3_BUCKET = "ops-insight-116626549437"
RAW_PREFIX = "raw"

# Valid values for the status column
VALID_STATUSES = ["success", "failure", "pending"]

# How many total rows to generate per day
ROWS_PER_DAY = 500

# What fraction of rows should be intentionally broken (roughly 10%)
BAD_ROW_FRACTION = 0.10


# ── Helper: build one clean row ───────────────────────────────────────────────

def make_clean_row(event_num: int, base_date: datetime) -> dict:
    """
    Returns a single valid event row as a dictionary.

    Args:
        event_num  : used to build a unique event_id
        base_date  : the date of this simulated pipeline run
    """
    # Spread events across a random time during the day
    random_seconds = random.randint(0, 86_399)  # 0..23:59:59
    timestamp = base_date + timedelta(seconds=random_seconds)

    return {
        "event_id":  f"EVT-{event_num:05d}",
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "user_id":   f"USR-{random.randint(1000, 9999)}",
        "amount":    round(random.uniform(1.00, 999.99), 2),
        "status":    random.choice(VALID_STATUSES),
    }


# ── Helper: corrupt a clean row with one specific flaw ───────────────────────

def inject_flaw(row: dict, flaw_type: str, base_date: datetime) -> dict:
    """
    Takes a clean row and corrupts one field based on flaw_type.
    Returns the modified row (the original dict is mutated).

    Args:
        row       : a clean row dict
        flaw_type : the kind of corruption to apply
        base_date : needed to calculate a future timestamp
    """
    if flaw_type == "missing_user_id":
        row["user_id"] = ""

    elif flaw_type == "negative_amount":
        row["amount"] = round(random.uniform(-999.99, -0.01), 2)

    elif flaw_type == "future_timestamp":
        # Push the timestamp 1-30 days into the future
        future = base_date + timedelta(days=random.randint(1, 30))
        row["timestamp"] = future.strftime("%Y-%m-%d %H:%M:%S")

    elif flaw_type == "invalid_status":
        # Use a value that is not in VALID_STATUSES
        row["status"] = random.choice(["unknown", "error", "cancelled", ""])

    elif flaw_type == "duplicate_event_id":
        # Force a low event number so it collides with an earlier row
        row["event_id"] = f"EVT-{random.randint(1, 50):05d}"

    return row


# ── Main: generate one day's raw file ────────────────────────────────────────

def generate_raw_file(date_str: str) -> str:
    """
    Generates a raw CSV for the given date and uploads it to S3.
    Returns the S3 key of the file that was written.

    Instead of writing to a local file we build the CSV in memory
    using io.StringIO, then upload the string content to S3.
    This is the standard pattern for writing to S3 from Python.

    Args:
        date_str : e.g. "2026-04-11"
    """
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    s3_key = f"{RAW_PREFIX}/{date_str}.csv"

    # Decide in advance which row indices will be bad
    total_bad = int(ROWS_PER_DAY * BAD_ROW_FRACTION)
    bad_indices = set(random.sample(range(ROWS_PER_DAY), total_bad))

    # The pool of flaw types we rotate through for variety
    flaw_types = [
        "missing_user_id",
        "negative_amount",
        "future_timestamp",
        "invalid_status",
        "duplicate_event_id",
    ]

    rows = []
    for i in range(ROWS_PER_DAY):
        row = make_clean_row(event_num=i + 1, base_date=base_date)

        if i in bad_indices:
            flaw = flaw_types[len(rows) % len(flaw_types)]
            row = inject_flaw(row, flaw, base_date)

        rows.append(row)

    # Build the CSV in memory instead of writing to disk
    # io.StringIO is an in-memory text buffer that behaves like a file
    fieldnames = ["event_id", "timestamp", "user_id", "amount", "status"]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    # Upload the in-memory CSV string to S3
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),       # the full CSV as a string
        ContentType="text/csv",
    )

    print(f"[generate_data] Uploaded {len(rows)} rows to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


# ── Entry point: generate the last 5 days ────────────────────────────────────

if __name__ == "__main__":
    today = datetime.today()

    # Generate 5 days of history so compare_runs has something to work with
    for days_ago in range(4, -1, -1):  # 4, 3, 2, 1, 0
        date_str = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        generate_raw_file(date_str)

    print(f"\n[generate_data] Done. Check s3://{S3_BUCKET}/{RAW_PREFIX}/ for output files.")
