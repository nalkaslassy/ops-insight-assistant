"""
tools.py
--------
Four inspector functions that the agent uses to read pipeline outputs from S3.

These functions do not reason or interpret — they just read files from S3
and return structured data. All reasoning happens in agent_loop.py.

Available tools:
  - get_run_summary(date)
  - get_log_snippets(date, keyword)
  - get_quarantine_samples(date, n)
  - compare_runs(date_a, date_b)
"""

import csv
import io
import json

import boto3

# ── S3 constants (must match pipeline.py) ─────────────────────────────────────

S3_BUCKET         = "ops-insight-116626549437"
SUMMARIES_PREFIX  = "summaries"
LOGS_PREFIX       = "logs"
QUARANTINE_PREFIX = "quarantine"


# ── S3 client ─────────────────────────────────────────────────────────────────
# Created once at module level so every tool reuses the same connection

s3 = boto3.client("s3")


# ── Tool 1: get_run_summary ───────────────────────────────────────────────────

def get_run_summary(date: str) -> dict:
    """
    Returns the full summary JSON for a given pipeline run date.
    This is the first tool the agent will usually call — it gives a
    high level picture of what happened on that day.

    Fetches from: s3://<bucket>/summaries/<date>.json

    Args:
        date : run date in YYYY-MM-DD format, e.g. "2026-04-12"

    Returns:
        dict with keys: date, total_rows, curated_rows, quarantined_rows,
                        quarantine_rate, failure_reasons, status, run_timestamp
    """
    key = f"{SUMMARIES_PREFIX}/{date}.json"

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content  = response["Body"].read().decode("utf-8")
        return json.loads(content)

    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"No summary found for {date} in S3. "
            f"Has the pipeline been run for this date?"
        )


# ── Tool 2: get_log_snippets ──────────────────────────────────────────────────

def get_log_snippets(date: str, keyword: str = "", max_lines: int = 20) -> list[str]:
    """
    Returns log lines from a pipeline run that contain the given keyword.
    If no keyword is provided, returns the last max_lines lines of the log.

    Fetches from: s3://<bucket>/logs/<date>.log

    Useful for searching for specific events like "ERROR", "quarantined",
    or a specific failure reason like "negative_amount".

    Args:
        date      : run date in YYYY-MM-DD format
        keyword   : string to search for in log lines (case-insensitive)
        max_lines : maximum number of lines to return (default 20)

    Returns:
        list of matching log line strings, up to max_lines
    """
    key = f"{LOGS_PREFIX}/{date}.log"

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content  = response["Body"].read().decode("utf-8")
        lines    = content.splitlines()

    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"No log found for {date} in S3.")

    if keyword:
        matches = [line for line in lines if keyword.lower() in line.lower()]
    else:
        matches = lines

    return matches[:max_lines]


# ── Tool 3: get_quarantine_samples ────────────────────────────────────────────

def get_quarantine_samples(date: str, n: int = 10, failure_reason: str = "") -> list[dict]:
    """
    Returns a sample of quarantined rows for a given date.
    Optionally filter by a specific failure_reason.

    Fetches from: s3://<bucket>/quarantine/<date>.csv

    This lets the agent look at actual bad rows, not just counts.
    Seeing real examples helps the model identify patterns.

    Args:
        date           : run date in YYYY-MM-DD format
        n              : number of rows to return (default 10)
        failure_reason : if provided, only return rows with this reason
                         e.g. "negative_amount", "missing_user_id"

    Returns:
        list of dicts, each representing one quarantined row.
        Each dict includes: event_id, timestamp, user_id, amount,
                            status, failure_reason
    """
    key = f"{QUARANTINE_PREFIX}/{date}.csv"

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content  = response["Body"].read().decode("utf-8")
        rows     = list(csv.DictReader(io.StringIO(content)))

    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"No quarantine file found for {date} in S3.")

    if failure_reason:
        rows = [r for r in rows if r.get("failure_reason") == failure_reason]

    return rows[:n]


# ── Tool 4: compare_runs ──────────────────────────────────────────────────────

def compare_runs(date_a: str, date_b: str) -> dict:
    """
    Compares two pipeline run summaries and returns a diff of key metrics.
    date_a is treated as the baseline (earlier run).
    date_b is treated as the current run being investigated.

    Calls get_run_summary() twice — no direct S3 access needed here
    since get_run_summary already handles the S3 fetch.

    Args:
        date_a : the earlier/baseline date, e.g. "2026-04-11"
        date_b : the later/current date,   e.g. "2026-04-12"

    Returns:
        dict with:
          - date_a, date_b             : the two dates compared
          - quarantine_rate_change     : how much the rate shifted
          - failure_reason_changes     : per-reason count diff
          - total_rows_change          : difference in row volume
          - summary_a, summary_b       : the full summaries for reference
    """
    summary_a = get_run_summary(date_a)
    summary_b = get_run_summary(date_b)

    rate_change = round(
        summary_b["quarantine_rate"] - summary_a["quarantine_rate"], 4
    )

    reasons_a   = summary_a.get("failure_reasons", {})
    reasons_b   = summary_b.get("failure_reasons", {})
    all_reasons = set(reasons_a.keys()) | set(reasons_b.keys())

    failure_reason_changes = {}
    for reason in sorted(all_reasons):
        count_a = reasons_a.get(reason, 0)
        count_b = reasons_b.get(reason, 0)
        failure_reason_changes[reason] = {
            date_a:   count_a,
            date_b:   count_b,
            "change": count_b - count_a,
        }

    return {
        "date_a":                 date_a,
        "date_b":                 date_b,
        "total_rows_change":      summary_b["total_rows"] - summary_a["total_rows"],
        "quarantine_rate_change": rate_change,
        "failure_reason_changes": failure_reason_changes,
        "summary_a":              summary_a,
        "summary_b":              summary_b,
    }
