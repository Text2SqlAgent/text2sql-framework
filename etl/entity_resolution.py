"""Generic entity-resolution pipeline.

Customer-agnostic. Operates on the silver.<entity>_master / aliases /
merge_candidates tables defined in etl/entity_resolution.sql.

Stages
------
1. bootstrap_aliases   — every distinct source-system id becomes its own
                         canonical_master row, mapped via aliases. Idempotent.
2. fuzzy_auto_merge    — collapse canonical_master rows whose normalized
                         names exceed `threshold` similarity (default 0.95).
                         Uses pg_trgm + the indexed canonical_name_norm column.
                         Conservative on purpose — under-merge is the safe default.
3. populate_merge_candidates  — diagnostic. Logs pairs in the [low, high]
                                 similarity band that the system did NOT
                                 auto-merge, for audit/observability.

What this module deliberately does NOT do
------------------------------------------
- Customer-driven review queues (we do the cleaning ourselves).
- LLM-assisted merging (could be added later as a separate `llm_assisted`
  match_method, but never bypasses the conservative default).
- Anything customer-specific. Source-table → entity mapping is passed in
  by the caller.

Usage
-----
    from etl.entity_resolution import run, EntitySource

    config: dict[str, EntitySource] = {
        "customer": {"table": "silver.clientes",
                     "id_col": "customer_branch_id",
                     "name_col": "trade_name"},
        ...
    }
    stats = run(engine, config)

Or via CLI (uses AltoControl config in etl.altocontrol_entity_config):

    uv run python -m etl.entity_resolution
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TypedDict

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE = REPO_ROOT / "etl" / "entity_resolution.sql"

DEFAULT_AUTO_MERGE_THRESHOLD = 0.95
DEFAULT_CANDIDATE_LOW = 0.80
DEFAULT_CANDIDATE_HIGH = 0.95


class EntitySource(TypedDict):
    """Customer-supplied mapping from an entity name to the silver table that holds it."""
    table: str       # e.g. "silver.clientes"
    id_col: str      # e.g. "customer_branch_id"
    name_col: str    # e.g. "trade_name"


def apply_schema(engine: Engine) -> None:
    """Apply etl/entity_resolution.sql to the target DB. Idempotent."""
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    # psycopg3 interprets unescaped '%' in raw SQL as parameter placeholders.
    # The schema is parameter-free, so double any '%' so they pass through literally.
    sql = sql.replace("%", "%%")
    with engine.connect() as conn:
        conn.exec_driver_sql(sql)
        conn.commit()


def bootstrap_aliases(engine: Engine, entity: str, source: EntitySource) -> int:
    """For every source row not already in <entity>_aliases, create a fresh
    <entity>_master row and an alias row pointing to it. Idempotent.

    Returns the count of new aliases created.
    """
    select_unmapped = text(f"""
        SELECT DISTINCT
            {source['id_col']}::TEXT AS source_id,
            COALESCE({source['name_col']}, '(unnamed)') AS source_name
        FROM {source['table']}
        WHERE {source['id_col']} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM silver.{entity}_aliases a
              WHERE a.source_id = {source['id_col']}::TEXT
          )
        ORDER BY source_id
    """)

    insert_master = text(f"""
        INSERT INTO silver.{entity}_master (canonical_name)
        VALUES (:name)
        RETURNING canonical_id
    """)

    insert_alias = text(f"""
        INSERT INTO silver.{entity}_aliases
            (source_id, canonical_id, source_name,
             match_method, confidence, reviewed_by, reviewed_at)
        VALUES (:sid, :cid, :sname, 'exact_id', 1.000, 'auto', NOW())
    """)

    count = 0
    with engine.begin() as conn:
        rows = conn.execute(select_unmapped).all()
        for row in rows:
            cid = conn.execute(insert_master, {"name": row.source_name}).scalar()
            conn.execute(insert_alias, {
                "sid": row.source_id, "cid": cid, "sname": row.source_name,
            })
            count += 1
    return count


def fuzzy_auto_merge(
    engine: Engine,
    entity: str,
    threshold: float = DEFAULT_AUTO_MERGE_THRESHOLD,
) -> int:
    """Auto-merge active <entity>_master rows whose normalized canonical_names
    are equal (similarity = 1.0 after UPPER + TRIM + collapsed whitespace).

    Uses an equi-join on the B-tree-indexed canonical_name_norm column, so
    this is O(N log N). The merge is conservative on purpose: only collapse
    pairs that normalize to identical strings. Near-misses (period vs no
    period, 'SA' vs 'S.A.', etc.) stay separate by default and surface in
    populate_merge_candidates instead.

    The `threshold` parameter is accepted for API compatibility but unused;
    exact-equality after normalization corresponds to similarity = 1.0,
    which is above any threshold a caller would set.

    Returns the count of merges performed.
    """
    del threshold  # accepted for API parity; this implementation is exact-match.

    find_pair = text(f"""
        SELECT a.canonical_id AS keep_id,
               b.canonical_id AS merge_id
        FROM silver.{entity}_master a
        JOIN silver.{entity}_master b
            ON a.canonical_name_norm = b.canonical_name_norm
           AND a.canonical_id < b.canonical_id
        WHERE a.is_active AND b.is_active
        ORDER BY a.canonical_id, b.canonical_id
        LIMIT 1
    """)

    repoint_aliases = text(f"""
        UPDATE silver.{entity}_aliases
        SET canonical_id = :keep,
            match_method = 'fuzzy_auto',
            confidence = 1.000,
            reviewed_by = 'auto',
            reviewed_at = NOW()
        WHERE canonical_id = :merge
    """)

    deactivate_master = text(f"""
        UPDATE silver.{entity}_master
        SET is_active = FALSE, updated_at = NOW()
        WHERE canonical_id = :merge
    """)

    merged = 0
    for _ in range(100_000):
        with engine.begin() as conn:
            pair = conn.execute(find_pair).first()
            if pair is None:
                break
            conn.execute(repoint_aliases, {
                "keep": pair.keep_id, "merge": pair.merge_id,
            })
            conn.execute(deactivate_master, {"merge": pair.merge_id})
            merged += 1
    return merged


def populate_merge_candidates(
    engine: Engine,
    entity: str,
    low: float = DEFAULT_CANDIDATE_LOW,
    high: float = DEFAULT_CANDIDATE_HIGH,
) -> int:
    """Log pairs of active masters whose normalized-name similarity falls in
    (low, high] into <entity>_merge_candidates for diagnostic review.
    Diagnostic only — does NOT trigger any merge. Existing rows are not
    overwritten (ON CONFLICT DO NOTHING). Returns count of new rows.

    Computes similarity() over all active pairs (no GIN-index probe), so
    this is O(N²) per entity. Acceptable for a once-per-pipeline diagnostic
    at typical mid-market customer scale (low thousands of masters per entity).
    """
    sql = text(f"""
        INSERT INTO silver.{entity}_merge_candidates
            (canonical_id_a, canonical_id_b, similarity, suggested_by, status)
        SELECT a.canonical_id, b.canonical_id,
               similarity(a.canonical_name_norm, b.canonical_name_norm),
               'pg_trgm',
               'pending'
        FROM silver.{entity}_master a
        JOIN silver.{entity}_master b
            ON a.canonical_id < b.canonical_id
        WHERE a.is_active AND b.is_active
          AND similarity(a.canonical_name_norm, b.canonical_name_norm) > :low
          AND similarity(a.canonical_name_norm, b.canonical_name_norm) <= :high
        ON CONFLICT (canonical_id_a, canonical_id_b) DO NOTHING
    """)

    with engine.begin() as conn:
        result = conn.execute(sql, {"low": low, "high": high})
        return result.rowcount or 0


def run(
    engine: Engine,
    entity_config: dict[str, EntitySource],
    auto_merge_threshold: float = DEFAULT_AUTO_MERGE_THRESHOLD,
    candidate_low: float = DEFAULT_CANDIDATE_LOW,
    candidate_high: float = DEFAULT_CANDIDATE_HIGH,
    apply_schema_first: bool = True,
) -> dict[str, dict]:
    """Run all stages for every configured entity. Returns per-entity stats."""
    if apply_schema_first:
        apply_schema(engine)

    stats: dict[str, dict] = {}
    for entity, source in entity_config.items():
        bootstrapped = bootstrap_aliases(engine, entity, source)
        merged = fuzzy_auto_merge(engine, entity, threshold=auto_merge_threshold)
        candidates = populate_merge_candidates(
            engine, entity, low=candidate_low, high=candidate_high,
        )
        stats[entity] = {
            "bootstrapped": bootstrapped,
            "merged": merged,
            "candidates_logged": candidates,
        }
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _main() -> int:
    try:
        from dotenv import load_dotenv
        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=True)
    except ImportError:
        pass

    db_url = (
        os.environ.get("APP_DB_URL")
        or os.environ.get("PENICOR_DB_URL")
        or os.environ.get("TEXT2SQL_DB")
    )
    if not db_url:
        print("[entity_resolution] No DB URL found in env "
              "(APP_DB_URL / PENICOR_DB_URL / TEXT2SQL_DB).", file=sys.stderr)
        return 1

    db_url = db_url.split("?")[0]

    try:
        from etl.altocontrol_entity_config import ENTITY_SOURCES
    except ImportError as e:
        print(f"[entity_resolution] Could not load AltoControl entity config: {e}",
              file=sys.stderr)
        return 2

    engine = create_engine(db_url)
    print(f"[entity_resolution] applying schema: {SCHEMA_FILE.name}", flush=True)
    print(f"[entity_resolution] entities: {list(ENTITY_SOURCES.keys())}", flush=True)

    stats = run(engine, ENTITY_SOURCES)
    print(f"[entity_resolution] done.", flush=True)
    for entity, s in stats.items():
        print(f"  {entity:12s}  bootstrapped={s['bootstrapped']:>6d}  "
              f"merged={s['merged']:>4d}  candidates={s['candidates_logged']:>4d}",
              flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
