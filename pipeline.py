"""
pipeline.py
-----------
Reads one raw CSV from S3, validates every row, and produces:
  - s3://<bucket>/curated/<date>.csv       clean rows that passed all checks
  - s3://<bucket>/quarantine/<date>.csv    bad rows with a failure_reason column added
  - s3://<bucket>/summaries/<date>.json    stats about this run
  - s3://<bucket>/logs/<date>.log          timestamped log of what happened

Run it directly:
    python pipeline.py 2026-04-12

Or import and call run_pipeline("2026-04-12") from other scripts.
"""

import csv
import io
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime

import boto3

# ── S3 constants ──────────────────────────────────────────────────────────────

S3_BUCKET      = "ops-insight-116626549437"
RAW_PREFIX       = "raw"
CURATED_PREFIX   = "curated"
QUARANTINE_PREFIX = "quarantine"
SUMMARIES_PREFIX = "summaries"
LOGS_PREFIX      = "logs"

# Valid values for the status column (must match generate_data.py)
VALID_STATUSES = {"success", "failure", "pending"}


# ── S3 helpers ────────────────────────────────────────────────────────────────

def s3_read_csv(s3_client, key: str) -> list[dict]:
    """
    Downloads a CSV from S3 and returns it as a list of dicts.
    This replaces: open(path) + csv.DictReader

    Args:
        s3_client : boto3 S3 client
        key       : S3 key e.g. "raw/2026-04-12.csv"
    """
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    content  = response["Body"].read().decode("utf-8")
    reader   = csv.DictReader(io.StringIO(content))
    return list(reader)


def s3_write_csv(s3_client, key: str, rows: list[dict], fieldnames: list[str]) -> None:
    """
    Writes a list of dicts as a CSV and uploads it to S3.
    This replaces: open(path, "w") + csv.DictWriter

    Args:
        s3_client  : boto3 S3 client
        key        : S3 key e.g. "curated/2026-04-12.csv"
        rows       : list of row dicts to write
        fieldnames : column order for the CSV
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="text/csv",
    )


def s3_write_json(s3_client, key: str, data: dict) -> None:
    """
    Serializes a dict to JSON and uploads it to S3.
    This replaces: open(path, "w") + json.dump

    Args:
        s3_client : boto3 S3 client
        key       : S3 key e.g. "summaries/2026-04-12.json"
        data      : dict to serialize
    """
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json",
    )


def s3_write_log(s3_client, key: str, log_lines: list[str]) -> None:
    """
    Uploads a list of log lines as a single .log file to S3.

    Unlike local logging where we write line by line as events happen,
    S3 uploads are one shot. So we collect all log lines in memory
    during the run and upload the complete file at the end.

    Args:
        s3_client : boto3 S3 client
        key       : S3 key e.g. "logs/2026-04-12.log"
        log_lines : list of formatted log strings
    """
    content = "\n".join(log_lines)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=content,
        ContentType="text/plain",
    )


# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logger(date_str: str) -> tuple[logging.Logger, list[str]]:
    """
    Creates a logger that writes to the console and also collects lines
    in a list so we can upload them to S3 at the end of the run.

    Returns both the logger and the log_lines list so run_pipeline
    can pass log_lines to s3_write_log when the run completes.

    Args:
        date_str : the date of this run
    """
    log_lines = []  # in-memory collector for S3 upload

    logger = logging.getLogger(f"pipeline.{date_str}")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger, log_lines

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler — shows INFO and above in the terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Memory handler — captures every log line into log_lines list
    class ListHandler(logging.Handler):
        def emit(self, record):
            log_lines.append(self.format(record))

    list_handler = ListHandler()
    list_handler.setFormatter(formatter)
    logger.addHandler(list_handler)

    return logger, log_lines


# ── Row validation ────────────────────────────────────────────────────────────

def validate_row(row: dict, seen_event_ids: set, run_date: datetime) -> str | None:
    """
    Checks a single row against all validation rules.
    Returns a failure_reason string if invalid, or None if the row is clean.

    This function does not change for S3 — it only works on row dicts,
    not files, so there is nothing to migrate here.

    Args:
        row            : a dict representing one CSV row
        seen_event_ids : set of event_ids already processed in this run
        run_date       : the date of this pipeline run (for timestamp checks)
    """

    # Rule 1: user_id must not be empty
    if not row.get("user_id", "").strip():
        return "missing_user_id"

    # Rule 2: amount must be a valid positive number
    try:
        amount = float(row["amount"])
        if amount <= 0:
            return "negative_amount"
    except (ValueError, KeyError):
        return "negative_amount"

    # Rule 3: timestamp must not be in the future
    try:
        event_time = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
        end_of_run_day = run_date.replace(hour=23, minute=59, second=59)
        if event_time > end_of_run_day:
            return "future_timestamp"
    except (ValueError, KeyError):
        return "future_timestamp"

    # Rule 4: status must be one of the allowed values
    if row.get("status", "").strip() not in VALID_STATUSES:
        return "invalid_status"

    # Rule 5: event_id must not have appeared earlier in this file
    event_id = row.get("event_id", "").strip()
    if event_id in seen_event_ids:
        return "duplicate_event_id"

    return None  # Row is clean


# ── Main pipeline function ────────────────────────────────────────────────────

def run_pipeline(date_str: str) -> dict:
    """
    Runs the full pipeline for one date.
    Reads raw CSV from S3, validates rows, writes all outputs back to S3.
    Returns the summary dict so callers can inspect the result.

    Args:
        date_str : e.g. "2026-04-12"
    """
    run_start  = datetime.now()
    run_date   = datetime.strptime(date_str, "%Y-%m-%d")
    s3         = boto3.client("s3")
    logger, log_lines = setup_logger(date_str)

    logger.info(f"Pipeline starting for date: {date_str}")

    # ── Step 1: Read the raw file from S3 ────────────────────────────────────

    raw_key = f"{RAW_PREFIX}/{date_str}.csv"
    logger.info(f"Reading raw file from s3://{S3_BUCKET}/{raw_key}")

    try:
        raw_rows = s3_read_csv(s3, raw_key)
    except s3.exceptions.NoSuchKey:
        logger.error(f"Raw file not found in S3: {raw_key}")
        raise FileNotFoundError(f"No raw file found for {date_str} in S3")

    logger.info(f"Read {len(raw_rows)} raw rows")

    # ── Step 2: Validate every row ────────────────────────────────────────────
    # This section is identical to the local version — no S3 changes needed

    curated_rows    = []
    quarantine_rows = []
    seen_event_ids  = set()
    failure_counts  = defaultdict(int)

    for i, row in enumerate(raw_rows):
        failure_reason = validate_row(row, seen_event_ids, run_date)

        if failure_reason:
            row["failure_reason"] = failure_reason
            quarantine_rows.append(row)
            failure_counts[failure_reason] += 1
            logger.debug(
                f"Row {i+1} quarantined | event_id={row.get('event_id')} "
                f"| reason={failure_reason}"
            )
        else:
            curated_rows.append(row)
            seen_event_ids.add(row["event_id"])

    logger.info(
        f"Validation complete | curated={len(curated_rows)} "
        f"quarantined={len(quarantine_rows)}"
    )

    for reason, count in sorted(failure_counts.items()):
        logger.info(f"  Failure reason: {reason} = {count} rows")

    # ── Step 3: Write curated CSV to S3 ──────────────────────────────────────

    curated_key    = f"{CURATED_PREFIX}/{date_str}.csv"
    curated_fields = ["event_id", "timestamp", "user_id", "amount", "status"]
    s3_write_csv(s3, curated_key, curated_rows, curated_fields)
    logger.info(f"Curated file written: s3://{S3_BUCKET}/{curated_key}")

    # ── Step 4: Write quarantine CSV to S3 ───────────────────────────────────

    quarantine_key    = f"{QUARANTINE_PREFIX}/{date_str}.csv"
    quarantine_fields = ["event_id", "timestamp", "user_id", "amount", "status", "failure_reason"]
    s3_write_csv(s3, quarantine_key, quarantine_rows, quarantine_fields)
    logger.info(f"Quarantine file written: s3://{S3_BUCKET}/{quarantine_key}")

    # ── Step 5: Build and write summary JSON to S3 ────────────────────────────

    total_rows      = len(raw_rows)
    quarantine_rate = round(len(quarantine_rows) / total_rows, 4) if total_rows else 0

    summary = {
        "date":             date_str,
        "total_rows":       total_rows,
        "curated_rows":     len(curated_rows),
        "quarantined_rows": len(quarantine_rows),
        "quarantine_rate":  quarantine_rate,
        "failure_reasons":  dict(failure_counts),
        "status":           "completed",
        "run_timestamp":    run_start.strftime("%Y-%m-%d %H:%M:%S"),
    }

    summary_key = f"{SUMMARIES_PREFIX}/{date_str}.json"
    s3_write_json(s3, summary_key, summary)
    logger.info(f"Summary written: s3://{S3_BUCKET}/{summary_key}")

    logger.info(
        f"Pipeline finished | quarantine_rate={quarantine_rate:.1%} "
        f"| duration={(datetime.now() - run_start).total_seconds():.2f}s"
    )

    # ── Step 6: Upload the collected log lines to S3 ──────────────────────────
    # This happens last so the log includes every line from the run

    log_key = f"{LOGS_PREFIX}/{date_str}.log"
    s3_write_log(s3, log_key, log_lines)

    return summary


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    result = run_pipeline(date_arg)

    print("\nRun summary:")
    print(json.dumps(result, indent=2))
