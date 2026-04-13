# Ops Insight Assistant

An AI-powered pipeline operations assistant that investigates data quality issues using evidence-based reasoning.

The system simulates a real daily data pipeline, processes and validates incoming records, stores outputs on AWS S3, and exposes an AI agent that can answer operational questions like:

- *"Why did the April 12 run have a high quarantine rate?"*
- *"What changed between yesterday and today?"*
- *"What is the likely root cause and what should I fix first?"*

---

## What Makes This Different From a Chatbot

The agent never guesses. It calls Python tool functions that read real pipeline outputs from S3, builds evidence from the results, and only then forms a structured conclusion.

Every answer includes:
- **summary** — plain English explanation
- **root_cause** — specific technical finding
- **evidence** — facts drawn directly from tool outputs
- **confidence** — high / medium / low with reasoning
- **recommended_fixes** — ordered list of actions to take
- **next_checks** — what to investigate if fixes do not resolve the issue

---

## Architecture

```
generate_data.py     Simulates an upstream system dropping daily event files
                     Uploads raw CSVs to S3

pipeline.py          Reads raw CSV from S3
                     Validates every row against 5 rules
                     Routes clean rows to curated, bad rows to quarantine
                     Writes 4 outputs back to S3:
                       - curated/<date>.csv
                       - quarantine/<date>.csv   (with failure_reason column)
                       - summaries/<date>.json   (run stats)
                       - logs/<date>.log         (timestamped audit trail)

tools.py             Four functions the agent uses to read pipeline outputs
                       - get_run_summary(date)
                       - get_log_snippets(date, keyword)
                       - get_quarantine_samples(date, n)
                       - compare_runs(date_a, date_b)

agent_loop.py        Sends question + tools to Claude
                     Executes tool calls, returns results to Claude
                     Loops until Claude produces a structured answer
```

---

## The Flow End to End

```
1. generate_data.py runs   →  raw CSV uploaded to S3
2. pipeline.py runs        →  validates rows, writes 4 outputs to S3
3. You ask a question      →  agent calls tools, reads S3 evidence, answers
```

Each stage is independent. They communicate only through S3.

---

## Validation Rules

The pipeline catches five types of bad data:

| Rule | Failure Label |
|---|---|
| user_id cannot be empty | missing_user_id |
| amount must be greater than zero | negative_amount |
| timestamp cannot be in the future | future_timestamp |
| status must be success / failure / pending | invalid_status |
| event_id must be unique within the file | duplicate_event_id |

Bad rows are never modified or dropped silently. They are labeled with the exact failure reason and written to the quarantine file so engineers can investigate the upstream source.

---

## Setup

### Prerequisites
- Python 3.11+
- AWS account with S3 access
- Anthropic API key

### Install dependencies
```bash
pip install -r requirements.txt
```

### Configure AWS credentials
```bash
aws configure
```

### Set your Anthropic API key
```bash
# Mac / Linux
export ANTHROPIC_API_KEY=your-key-here

# Windows
set ANTHROPIC_API_KEY=your-key-here
```

### Update the S3 bucket name
In `generate_data.py`, `pipeline.py`, and `tools.py` replace:
```python
S3_BUCKET = "ops-insight-116626549437"
```
with your own bucket name.

### Create your S3 bucket
```bash
aws s3api create-bucket --bucket your-bucket-name --region us-east-1
```

---

## Running the System

### Step 1 — Generate raw data
```bash
python generate_data.py
```

### Step 2 — Run the pipeline
```bash
python pipeline.py 2026-04-12
```

### Step 3 — Ask the agent a question
Edit the question at the bottom of `agent_loop.py` then run:
```bash
python agent_loop.py
```

---

## Example Agent Output

```json
{
  "summary": "The April 12 run quarantined 48 of 500 rows at a 9.6% rate...",
  "root_cause": "Multi-category upstream data quality degradation with duplicate_event_id as the top contributor",
  "evidence": [
    "duplicate_event_id: 12 rows, up 50% from prior day",
    "negative_amount: values like -$975, -$893 across different users",
    "No ERROR lines in pipeline log — pipeline itself ran correctly"
  ],
  "confidence": "high — all categories confirmed with real quarantine row samples",
  "recommended_fixes": [
    "1. Investigate upstream event ID generator for replay or double-submit bug",
    "2. Audit transaction system for sign inversion on amount field"
  ],
  "next_checks": [
    "Run compare_runs over a 7-day window to find when degradation started"
  ]
}
```

---

## Key Design Decisions

**Pipeline is a gatekeeper, not a cleaner**
Bad rows are never modified or auto-corrected. They are labeled and quarantined so the upstream source can be fixed properly.

**Agent is evidence-based, not conversational**
The agent cannot answer without calling tools first. Every claim in the response maps to a specific tool output.

**S3 is the handoff layer**
The pipeline and the agent do not know about each other. They communicate only through the files S3 holds. This means each component can be replaced, scaled, or scheduled independently.

**Dispatcher bridges Claude and Python**
Claude cannot run Python directly. It requests tool calls by name. The dispatcher catches those requests, executes the right function, and returns the result. This is the same pattern used by MCP servers at a larger scale.

---

## AWS Cost

At this scale the project costs almost nothing:

| Service | Monthly Cost |
|---|---|
| S3 storage | ~$0.00 |
| S3 requests | ~$0.01 |
| Anthropic API (casual use) | ~$1-3 |

---

## Tech Stack

- Python 3.11+
- Claude (claude-sonnet-4-6) via Anthropic API
- AWS S3 via boto3
- Standard library: csv, json, logging, io, collections
