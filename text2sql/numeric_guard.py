"""Hallucination guard for analyst-style numeric narrative.

When the analyst subagent (or any LLM) writes a paragraph citing numbers
("revenue is up 12%", "DEVOTO HNOS at UYU 6.8M"), this module verifies
every numeric token in the narrative either appears in the source rows
or is derivable by simple aggregation (sums, counts, ranks, percentages).
Numbers that fail the check are returned as `unverified` so the caller
can append a warning, retry, or reject.

Why deterministic, not LLM-as-judge:
the 2026 SOB benchmark shows that even with citations or schema
constraints, LLMs hallucinate numeric leaves at meaningful rates.
Asking another LLM to check the first one's numbers introduces a second
hallucination surface. A regex+arithmetic post-check is cheap and
guarantees what it claims to guarantee.

What we deliberately allow:
- Numbers that appear literally in any row value (with rounding tolerance).
- Cumulative top-N sums (top-3, top-4, ... — common in narratives).
- Row count itself.
- Small integers 0-100 (ranks, percentages, counts of distinct entities).
- Common aggregate expressions: total of all rows, average, min, max.

What we flag:
- Numbers > 100 that aren't a row value, a top-N partial sum, or a basic
  aggregate. These are the most-likely hallucinations.
- Percentages outside 0-100.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Iterable


# Match numeric tokens. Handles 1,234.56 and 1.234,56 (UY/EU style).
# Excludes numbers immediately preceded/followed by letters (e.g. word "5th"
# is fine; "v3" or "h264" is not picked up). Allows leading minus.
_NUMBER_RE = re.compile(
    r"(?<![A-Za-z\d])"            # not preceded by letter or digit
    r"-?"                          # optional sign
    r"\d{1,3}(?:[.,]\d{3})*"       # 1, 12, 123, 1,234, 1.234, 12,345.678
    r"(?:[.,]\d+)?"                # optional fractional
    r"(?!\d)"                      # not followed by another digit
)


def _parse(token: str) -> Decimal | None:
    """Normalize thousands/decimal separators, return Decimal or None on failure."""
    raw = token.strip()
    if not raw:
        return None

    has_dot = "." in raw
    has_comma = "," in raw

    if has_dot and has_comma:
        # Both present: the LAST one is the decimal separator.
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif has_comma:
        # Could be EU decimal (12,5) or US thousands (1,234).
        parts = raw.split(",")
        if len(parts) == 2 and len(parts[1]) in (1, 2):
            raw = raw.replace(",", ".")
        else:
            raw = raw.replace(",", "")
    # If only dots, treat dots as decimal (US convention) — but US thousands
    # like 1.234 would be misparsed. Heuristic: if multiple dots, last is decimal.
    elif has_dot and raw.count(".") > 1:
        last = raw.rfind(".")
        raw = raw[:last].replace(".", "") + raw[last:]

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def extract_numbers(text: str) -> list[Decimal]:
    """Pull every numeric token out of free-form text."""
    out: list[Decimal] = []
    for token in _NUMBER_RE.findall(text or ""):
        d = _parse(token)
        if d is not None:
            out.append(d)
    return out


def _row_numbers(rows: list[dict]) -> list[Decimal]:
    """All numeric values across rows (ignoring None / non-numeric fields)."""
    out: list[Decimal] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for v in row.values():
            if v is None or isinstance(v, bool):
                continue
            if isinstance(v, (int, float, Decimal)):
                try:
                    out.append(Decimal(str(v)))
                except InvalidOperation:
                    pass
    return out


def _build_allowed(rows: list[dict]) -> set[Decimal]:
    """Build the set of numeric values the narrative is allowed to cite."""
    raw = _row_numbers(rows)
    allowed: set[Decimal] = set(raw)
    # Common rounding granularities (display formats vary)
    for v in raw:
        allowed.add(round(v, 2))
        allowed.add(round(v, 0))
        # Round to thousands and millions for display ("UYU 6.8M")
        if v.copy_abs() >= 1000:
            allowed.add(round(v / Decimal(1000), 1) * Decimal(1000))
            allowed.add(round(v / Decimal(1000), 2) * Decimal(1000))
        if v.copy_abs() >= 1_000_000:
            allowed.add(round(v / Decimal(1_000_000), 1) * Decimal(1_000_000))
            allowed.add(round(v / Decimal(1_000_000), 2) * Decimal(1_000_000))
            # Display form: "6.8M" cited as Decimal('6.8')
            allowed.add(round(v / Decimal(1_000_000), 1))
            allowed.add(round(v / Decimal(1_000_000), 2))

    # Row count
    allowed.add(Decimal(len(rows)))

    # Cumulative top-N partial sums on each numeric column independently.
    # Catches "the top 4 customers total UYU 23.8M" type claims.
    if rows and isinstance(rows[0], dict):
        for col in rows[0].keys():
            try:
                col_vals = [Decimal(str(r[col])) for r in rows
                            if r.get(col) is not None
                            and isinstance(r[col], (int, float, Decimal))
                            and not isinstance(r[col], bool)]
            except (InvalidOperation, TypeError):
                continue
            if not col_vals:
                continue
            # Sort descending and accumulate (typical "top N" framing)
            for sorted_dir in (sorted(col_vals, reverse=True), sorted(col_vals)):
                cumulative = Decimal(0)
                for v in sorted_dir:
                    cumulative += v
                    allowed.add(cumulative)
                    allowed.add(round(cumulative, 2))
                    if cumulative.copy_abs() >= 1_000_000:
                        allowed.add(round(cumulative / Decimal(1_000_000), 1))
                        allowed.add(round(cumulative / Decimal(1_000_000), 2))

            # Total / average / min / max
            total = sum(col_vals, Decimal(0))
            allowed.add(total)
            allowed.add(round(total, 2))
            allowed.add(min(col_vals))
            allowed.add(max(col_vals))
            avg = total / Decimal(len(col_vals))
            allowed.add(round(avg, 2))
            allowed.add(round(avg, 0))
    return allowed


def _within_tolerance(a: Decimal, b: Decimal, rel_tol: float) -> bool:
    """True if a and b agree within a relative tolerance."""
    if a == b:
        return True
    scale = max(a.copy_abs(), b.copy_abs(), Decimal(1))
    diff = (a - b).copy_abs()
    return diff <= scale * Decimal(str(rel_tol))


def validate(
    narrative: str,
    rows: list[dict],
    rel_tol: float = 0.01,
) -> list[Decimal]:
    """Return the list of numbers cited in narrative that the source rows
    do not justify. Empty list = narrative checks out.

    Small integers (|n| <= 100, integer-valued) are always permitted —
    they're typically ranks, percentages, or counts of distinct entities,
    which are derivable from the data shape itself.
    """
    if not narrative or not rows:
        return []

    cited = extract_numbers(narrative)
    if not cited:
        return []

    allowed = _build_allowed(rows)

    unverified: list[Decimal] = []
    for n in cited:
        # Permit small integers (ranks, %s, distinct counts)
        if n.copy_abs() <= 100 and n == n.to_integral_value():
            continue
        if any(_within_tolerance(n, a, rel_tol) for a in allowed):
            continue
        unverified.append(n)
    return unverified


def format_warning(unverified: Iterable[Decimal]) -> str:
    """Build a human-readable warning footer for the narrative."""
    nums = list(unverified)
    if not nums:
        return ""
    sample = ", ".join(str(n) for n in nums[:5])
    extra = f" (+{len(nums) - 5} more)" if len(nums) > 5 else ""
    return (
        f"\n\n⚠️ **Verification warning**: the following numbers in the "
        f"commentary were not found in the source data within tolerance "
        f"and may be hallucinated: {sample}{extra}. Treat these figures with "
        f"caution and verify against the SQL result rows above."
    )
