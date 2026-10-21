"""Pretty-print traces.jsonl so SQL, tool results, and reasoning are readable.

Run from the repo root:

    python examples/parse_traces.py
    python examples/parse_traces.py traces.jsonl
    python examples/parse_traces.py --summary
    python examples/parse_traces.py -n 2
    python examples/parse_traces.py --json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

DEFAULT_TRACE_FILE = Path("traces.jsonl")
SEPARATOR = "=" * 72
SUB = "-" * 72


def load_traces(path: Path) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                traces.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"{path}:{line_no}: invalid JSON: {exc}", file=sys.stderr)
    return traces


def indent_block(text: str | None, prefix: str = "    ") -> str:
    if not text:
        return f"{prefix}(empty)"
    return "\n".join(prefix + row if row else prefix for row in str(text).splitlines())


def format_tool_call(i: int, call: dict[str, Any]) -> str:
    name = call.get("name", "?")
    args = call.get("arguments") or {}
    sql = args.get("sql")
    other_args = {k: v for k, v in args.items() if k != "sql"}
    exec_ms = call.get("execution_ms")
    think_ms = call.get("llm_think_ms")
    preview = call.get("result_preview")

    lines = [f"  [{i}] {name}"]
    if exec_ms is not None or think_ms is not None:
        timing = []
        if exec_ms is not None:
            timing.append(f"exec={exec_ms}ms")
        if think_ms is not None:
            timing.append(f"think={think_ms}ms")
        lines.append(f"      ({', '.join(timing)})")
    if sql:
        lines.append("      SQL:")
        lines.append(indent_block(sql, "        "))
    if other_args:
        lines.append(f"      args: {json.dumps(other_args, ensure_ascii=False)}")
    if preview:
        lines.append("      result:")
        lines.append(indent_block(preview, "        "))
    return "\n".join(lines)


def format_trace(trace: dict[str, Any], index: int, total: int) -> str:
    ok = trace.get("success")
    status = "OK" if ok else "FAIL"
    parts = [
        SEPARATOR,
        f"Trace {index}/{total}  [{status}]",
        SEPARATOR,
        f"Question:  {trace.get('question')}",
        f"Duration:  {trace.get('duration_seconds')}s",
        (
            f"Tokens:    in={trace.get('input_tokens')}  "
            f"out={trace.get('output_tokens')}"
        ),
        (
            f"Calls:     tools={trace.get('total_tool_calls')}  "
            f"llm={trace.get('llm_iterations')}  "
            f"sql={trace.get('sql_attempts')}  "
            f"sql_errors={trace.get('sql_errors')}  "
            f"schema={trace.get('schema_queries')}  "
            f"backtracks={trace.get('schema_backtracking_count')}"
        ),
    ]
    if trace.get("error"):
        parts.append(f"Error:     {trace['error']}")

    parts += [SUB, "Final SQL", SUB, indent_block(trace.get("final_sql"))]

    tool_calls = trace.get("tool_calls") or []
    if tool_calls:
        parts += [SUB, f"Tool calls ({len(tool_calls)})", SUB]
        parts.extend(format_tool_call(i, c) for i, c in enumerate(tool_calls, start=1))

    joins = trace.get("join_attempts") or []
    if joins:
        parts += [SUB, "Joins", SUB]
        for j in joins:
            mark = "ok" if j.get("success") else "fail"
            err = f"  {j['error_message']}" if j.get("error_message") else ""
            parts.append(
                f"  [{mark}] {j.get('left_table')} -> {j.get('right_table')}  "
                f"ON {j.get('join_condition')}{err}"
            )

    sql_errors = trace.get("sql_errors_structured") or []
    if sql_errors:
        parts += [SUB, "SQL errors", SUB]
        for err in sql_errors:
            parts.append(f"  {json.dumps(err, ensure_ascii=False)}")

    lookups = trace.get("example_lookups") or []
    if lookups:
        parts += [SUB, "Example lookups", SUB]
        for item in lookups:
            parts.append(f"  {item}")

    searched = trace.get("columns_searched_vs_found") or []
    if searched:
        parts += [SUB, "Columns searched vs found", SUB]
        for item in searched:
            parts.append(f"  {item}")

    terms = trace.get("search_terms_used") or []
    if terms:
        parts += [SUB, "Search terms", SUB]
        parts.append(f"  {', '.join(str(t) for t in terms)}")

    steps = trace.get("llm_reasoning_steps") or []
    if steps:
        parts += [SUB, f"LLM reasoning ({len(steps)} step(s))", SUB]
        for i, step in enumerate(steps, start=1):
            parts.append(f"  --- step {i} ---")
            parts.append(indent_block(step))

    return "\n".join(parts)


def format_summary(traces: list[dict[str, Any]]) -> str:
    header = (
        f"{'#':>3}  {'ok':<4}  {'dur':>7}  {'tools':>5}  "
        f"{'sql_err':>7}  {'in_tok':>7}  {'out_tok':>7}  question"
    )
    rows = [header, SUB]
    for i, t in enumerate(traces, start=1):
        ok = "yes" if t.get("success") else "no"
        q = (t.get("question") or "").replace("\n", " ")
        if len(q) > 72:
            q = q[:69] + "..."
        rows.append(
            f"{i:>3}  {ok:<4}  {t.get('duration_seconds') or 0:7.2f}  "
            f"{t.get('total_tool_calls') or 0:5}  "
            f"{t.get('sql_errors') or 0:7}  "
            f"{t.get('input_tokens') or 0:7}  "
            f"{t.get('output_tokens') or 0:7}  {q}"
        )
    ok_n = sum(1 for t in traces if t.get("success"))
    rows.append(SUB)
    rows.append(f"{ok_n}/{len(traces)} succeeded")
    return "\n".join(rows)


def dumps_readable(obj: Any) -> str:
    """json.dumps with indent, then unescape \\n so SQL is readable."""
    return json.dumps(obj, indent=2, ensure_ascii=False).replace("\\n", "\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path",
        nargs="?",
        type=Path,
        default=DEFAULT_TRACE_FILE,
        help="JSONL trace file (default: traces.jsonl)",
    )
    parser.add_argument(
        "-n",
        "--index",
        type=int,
        metavar="N",
        help="Show only trace N (1-based)",
    )
    parser.add_argument(
        "-s",
        "--summary",
        action="store_true",
        help="One-line summary of each trace",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Pretty-print raw JSON with unescaped newlines",
    )
    return parser.parse_args(argv)


def select_traces(
    traces: list[dict[str, Any]], index: int | None
) -> tuple[list[tuple[int, dict[str, Any]]], int]:
    total = len(traces)
    if index is None:
        return [(i, t) for i, t in enumerate(traces, start=1)], total
    if index < 1 or index > total:
        raise IndexError(f"Index {index} out of range (1-{total})")
    return [(index, traces[index - 1])], total


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.path.exists():
        print(f"File not found: {args.path}", file=sys.stderr)
        return 1

    traces = load_traces(args.path)
    if not traces:
        print(f"No traces in {args.path}", file=sys.stderr)
        return 1

    try:
        selected, total = select_traces(traces, args.index)
    except IndexError as exc:
        print(exc, file=sys.stderr)
        return 1

    if args.summary:
        print(format_summary(traces))
        return 0

    if args.json:
        for _, trace in selected:
            print(dumps_readable(trace))
            print()
        return 0

    for i, trace in selected:
        print(format_trace(trace, i, total))
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
