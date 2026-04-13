"""
agent_loop.py
-------------
The AI reasoning engine for Ops Insight Assistant.

This file does three things:
  1. Defines the system prompt  — tells Claude how to behave
  2. Defines the tool schemas   — tells Claude what tools exist and what they do
  3. Runs the agent loop        — sends messages, executes tool calls, collects answers

Claude never opens a file directly. It requests a tool call, your Python
code executes it from tools.py, and the result is sent back to Claude.
That cycle repeats until Claude has enough evidence to answer.
"""

import json
import os
import anthropic

# Import the four tool functions Claude is allowed to call
from tools import (
    get_run_summary,
    get_log_snippets,
    get_quarantine_samples,
    compare_runs,
)


# ── 1. System Prompt ──────────────────────────────────────────────────────────
#
# This is the instruction set Claude reads before seeing any question.
# It defines the role, the rules, and the required output format.
# Without this, Claude would behave like a generic chatbot.

SYSTEM_PROMPT = """
You are an expert data pipeline operations assistant.

Your job is to investigate pipeline run issues by reading evidence from tools.
You must never state a root cause, draw a conclusion, or make a recommendation
without first calling tools to gather supporting evidence.

Investigation rules:
- Always call get_run_summary first to understand the overall picture
- If the quarantine rate is elevated, call get_quarantine_samples to see real examples
- If the question involves change over time, call compare_runs
- If you need to understand the sequence of events, call get_log_snippets
- Call tools in whatever order makes sense for the question
- You may call the same tool more than once with different arguments

When you have gathered enough evidence, respond with ONLY a JSON object in
exactly this format — no extra text before or after:

{
  "summary": "one paragraph plain English explanation of what happened",
  "root_cause": "the specific technical finding that best explains the issue",
  "evidence": [
    "fact 1 drawn directly from tool output",
    "fact 2 drawn directly from tool output"
  ],
  "confidence": "high | medium | low — followed by one sentence explaining why",
  "recommended_fixes": [
    "1. first thing to try",
    "2. second thing to try"
  ],
  "next_checks": [
    "what to investigate if the recommended fixes do not resolve the issue"
  ]
}
"""


# ── 2. Tool Schemas ───────────────────────────────────────────────────────────
#
# These tell Claude what tools exist, what each one does, and what
# arguments to pass. Claude reads these descriptions to decide which
# tool to call for a given question.
#
# This is NOT the Python function — it is the description Claude sees.
# The actual Python function lives in tools.py.

TOOL_SCHEMAS = [
    {
        "name": "get_run_summary",
        "description": (
            "Returns the full summary for a pipeline run on a given date. "
            "Use this first for any question about a specific run. "
            "Returns total rows, curated rows, quarantined rows, quarantine rate, "
            "and a breakdown of failure reasons with counts."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Run date in YYYY-MM-DD format, e.g. 2026-04-12",
                }
            },
            "required": ["date"],
        },
    },
    {
        "name": "get_log_snippets",
        "description": (
            "Returns log lines from a pipeline run that match a keyword. "
            "Use this to find specific events, errors, or patterns in the run log. "
            "Good keywords: 'ERROR', 'quarantined', 'negative_amount', 'missing_user_id'. "
            "If no keyword is provided, returns the last 20 lines of the log."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Run date in YYYY-MM-DD format",
                },
                "keyword": {
                    "type": "string",
                    "description": "String to search for in log lines (case-insensitive)",
                },
                "max_lines": {
                    "type": "integer",
                    "description": "Maximum number of lines to return (default 20)",
                },
            },
            "required": ["date"],
        },
    },
    {
        "name": "get_quarantine_samples",
        "description": (
            "Returns a sample of quarantined rows for a given date. "
            "Use this to see what actual bad rows look like. "
            "Optionally filter by failure_reason to focus on one issue type. "
            "Valid failure reasons: missing_user_id, negative_amount, "
            "future_timestamp, invalid_status, duplicate_event_id."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Run date in YYYY-MM-DD format",
                },
                "n": {
                    "type": "integer",
                    "description": "Number of rows to return (default 10)",
                },
                "failure_reason": {
                    "type": "string",
                    "description": "Filter to only rows with this failure reason (optional)",
                },
            },
            "required": ["date"],
        },
    },
    {
        "name": "compare_runs",
        "description": (
            "Compares two pipeline runs and returns a diff of key metrics. "
            "Use this when the question involves change over time, trends, "
            "or whether something is getting better or worse. "
            "date_a is the baseline (earlier), date_b is the current run."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "date_a": {
                    "type": "string",
                    "description": "Baseline date in YYYY-MM-DD format (earlier run)",
                },
                "date_b": {
                    "type": "string",
                    "description": "Current date in YYYY-MM-DD format (later run)",
                },
            },
            "required": ["date_a", "date_b"],
        },
    },
]


# ── 3. Tool Dispatcher ────────────────────────────────────────────────────────
#
# When Claude requests a tool call, this function receives the tool name
# and arguments, calls the right Python function, and returns the result.
#
# This is the bridge between what Claude asks for and what tools.py executes.

def dispatch_tool(tool_name: str, tool_input: dict) -> str:
    """
    Executes the tool Claude requested and returns the result as a JSON string.
    Claude receives the string, reads it as evidence, and continues reasoning.

    Args:
        tool_name  : name of the tool Claude wants to call
        tool_input : arguments Claude wants to pass to that tool

    Returns:
        JSON string of the tool result
    """
    print(f"  [tool call] {tool_name}({tool_input})")

    if tool_name == "get_run_summary":
        result = get_run_summary(**tool_input)

    elif tool_name == "get_log_snippets":
        result = get_log_snippets(**tool_input)

    elif tool_name == "get_quarantine_samples":
        result = get_quarantine_samples(**tool_input)

    elif tool_name == "compare_runs":
        result = compare_runs(**tool_input)

    else:
        result = {"error": f"Unknown tool: {tool_name}"}

    return json.dumps(result, indent=2)


# ── 4. The Agent Loop ─────────────────────────────────────────────────────────
#
# This is the core of the file. It sends the question to Claude, handles
# tool call requests by executing them and returning results, and keeps
# looping until Claude produces the final structured answer.

def run_agent(question: str) -> dict:
    """
    Runs the full agent loop for a given question.
    Returns the structured response as a Python dict.

    The loop:
      1. Send question + tools to Claude
      2. If Claude returns a tool call → execute it → send result back → repeat
      3. If Claude returns text → parse as final answer → return

    Args:
        question : the natural language question to investigate

    Returns:
        dict with keys: summary, root_cause, evidence,
                        confidence, recommended_fixes, next_checks
    """
    client = anthropic.Anthropic()

    # The conversation history — grows as tool calls are made
    messages = [
        {"role": "user", "content": question}
    ]

    print(f"\nQuestion: {question}")
    print("-" * 60)

    # Keep looping until Claude gives a final text response
    while True:

        # Send the current conversation to Claude
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOL_SCHEMAS,
            messages=messages,
        )

        # Check why Claude stopped responding
        stop_reason = response.stop_reason

        # ── Case 1: Claude wants to call one or more tools ────────────────────
        if stop_reason == "tool_use":

            # Add Claude's response (containing tool requests) to history
            messages.append({
                "role": "assistant",
                "content": response.content
            })

            # Process every tool call Claude requested in this turn
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    # Execute the tool and get the result
                    result_str = dispatch_tool(block.name, block.input)

                    # Package the result in the format Claude expects
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_str,
                    })

            # Send all tool results back to Claude in one message
            messages.append({
                "role": "user",
                "content": tool_results
            })

            # Loop back — Claude will read the results and decide what to do next

        # ── Case 2: Claude is done and produced a final answer ────────────────
        elif stop_reason == "end_turn":

            # Extract the text content from the response
            final_text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    final_text = block.text
                    break

            # Parse the JSON answer Claude returned
            try:
                # Strategy: find the first { and last } in the response.
                # This handles cases where Claude adds text before or after
                # the JSON, or wraps it in a markdown code block.
                start = final_text.find("{")
                end   = final_text.rfind("}") + 1

                if start == -1 or end == 0:
                    raise json.JSONDecodeError("No JSON object found", final_text, 0)

                clean = final_text[start:end]
                answer = json.loads(clean)
                return answer

            except json.JSONDecodeError:
                # If we still cannot parse, return the raw text as the summary
                return {
                    "summary": final_text,
                    "root_cause": "Could not parse structured response",
                    "evidence": [],
                    "confidence": "low — response was not in expected format",
                    "recommended_fixes": [],
                    "next_checks": ["Re-run the agent with a more specific question"],
                }

        # ── Case 3: Something unexpected happened ─────────────────────────────
        else:
            return {
                "summary": f"Agent stopped unexpectedly: stop_reason={stop_reason}",
                "root_cause": "Unknown",
                "evidence": [],
                "confidence": "low",
                "recommended_fixes": [],
                "next_checks": [],
            }


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Example question — change this to ask anything about your pipeline runs
    question = "Why did the April 12 2026 pipeline run have a high quarantine rate?"

    answer = run_agent(question)

    print("\n" + "=" * 60)
    print("AGENT RESPONSE")
    print("=" * 60)
    print(json.dumps(answer, indent=2))
