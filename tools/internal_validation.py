"""Internal data-correctness sanity check.

Runs the same validation questions against:
  - bronze.ac_*  (raw copy of AltoControl, no transforms)
  - gold.v_*     (our agent-facing layer, with entity resolution)

Prints both side-by-side to a markdown file we can read and diff. The point
is to convince *ourselves* the pipeline matches the source before showing
anything to the customer — if bronze and gold disagree on something we
didn't intentionally transform (dedup, sign-flipping credit notes), that's
a bug.

Usage:
  uv run python tools/internal_validation.py
  uv run python tools/internal_validation.py --output reports/internal.md
"""
from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal
from io import StringIO
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


# (title, explanation, bronze_sql, gold_sql)
CHECKS: list[tuple[str, str, str, str]] = [
    (
        "Top 10 customers by net revenue",
        "Bronze sums raw `neto * afecta_venta` per id_sucursal (i.e., per branch — "
        "no dedup). Gold groups by canonical entity. Big chains (Devoto, Disco) "
        "should collapse from many branches into one canonical row.",
        # bronze: raw, by id_sucursal
        """
        SELECT
          COALESCE(NULLIF(TRIM(b.razon), ''), TRIM(b.nombre)) AS name,
          ROUND(SUM(NULLIF(TRIM(v.neto), '')::NUMERIC * NULLIF(TRIM(v.afecta_venta), '')::INT)::NUMERIC, 2) AS net_revenue,
          COUNT(DISTINCT v.id_factura) AS invoices,
          COUNT(DISTINCT v.id_sucursal) AS branches
        FROM bronze.ac_ventas v
        JOIN bronze.ac_clientes b ON TRIM(b.id_sucursal) = TRIM(v.id_sucursal)
        WHERE TRIM(v.empresa) = 'Penicor'
        GROUP BY 1
        ORDER BY 2 DESC NULLS LAST
        LIMIT 10
        """,
        # gold: canonical
        """
        SELECT customer_name AS name,
               ROUND(net_cents / 100.0, 2) AS net_revenue,
               invoice_count AS invoices,
               source_branch_count AS branches
        FROM gold.v_revenue_by_customer
        ORDER BY net_cents DESC
        LIMIT 10
        """,
    ),
    (
        "Customer count",
        "Bronze: rows in ac_clientes (one per branch). Gold: deduped legal entities. "
        "Big difference is expected.",
        "SELECT COUNT(*) AS bronze_branch_rows FROM bronze.ac_clientes",
        "SELECT COUNT(*) AS gold_canonical_customers FROM gold.v_customers",
    ),
    (
        "Total AR by currency",
        "Bronze: SUM(deuda * 100) on rows where deuda > 0. Gold: same but via the "
        "view. These should match exactly — AR isn't deduped at line level.",
        """
        SELECT TRIM(moneda) AS currency,
               ROUND(SUM(NULLIF(TRIM(deuda), '')::NUMERIC), 2) AS owed,
               COUNT(*) AS docs
        FROM bronze.ac_deuda_por_cliente
        WHERE TRIM(empresa) = 'Penicor'
          AND NULLIF(TRIM(deuda), '')::NUMERIC > 0
        GROUP BY 1
        ORDER BY 2 DESC
        """,
        """
        SELECT currency_code AS currency,
               ROUND(total_owed_cents / 100.0, 2) AS owed,
               open_doc_count AS docs
        FROM gold.v_ar_total
        ORDER BY total_owed_cents DESC
        """,
    ),
    (
        "Monthly revenue (UYU only)",
        "Both should agree — we only dedup customers/products, not lines. "
        "If totals diverge there's a sign or filter bug.",
        """
        SELECT TO_CHAR(DATE_TRUNC('month', fecha::DATE), 'YYYY-MM') AS month,
               ROUND(SUM(NULLIF(TRIM(neto), '')::NUMERIC * NULLIF(TRIM(afecta_venta), '')::INT)::NUMERIC, 2) AS net_revenue,
               COUNT(DISTINCT id_factura) AS invoices
        FROM bronze.ac_ventas
        WHERE TRIM(empresa) = 'Penicor' AND TRIM(moneda) = 'UYU'
        GROUP BY 1
        ORDER BY 1 DESC
        """,
        """
        SELECT TO_CHAR(month, 'YYYY-MM') AS month,
               ROUND(net_cents / 100.0, 2) AS net_revenue,
               invoice_count AS invoices
        FROM gold.v_revenue_monthly
        WHERE currency_code = 'UYU'
        ORDER BY month DESC
        """,
    ),
    (
        "Top 10 most overdue customers",
        "Bronze: raw oldest unpaid invoices per branch. Gold: rolled up to canonical "
        "customer. The top USD line (GIBUR S.A.) should appear in both.",
        """
        SELECT
          COALESCE(NULLIF(TRIM(b.razon), ''), TRIM(b.nombre)) AS name,
          TRIM(d.moneda) AS currency,
          ROUND(SUM(NULLIF(TRIM(d.deuda), '')::NUMERIC), 2) AS owed,
          MAX((CURRENT_DATE - d.vencimiento::DATE))::INT AS max_days_overdue
        FROM bronze.ac_deuda_por_cliente d
        JOIN bronze.ac_clientes b ON TRIM(b.id_sucursal) = TRIM(d.id_sucursal)
        WHERE TRIM(d.empresa) = 'Penicor'
          AND NULLIF(TRIM(d.deuda), '')::NUMERIC > 0
          AND d.vencimiento IS NOT NULL
          AND (CURRENT_DATE - d.vencimiento::DATE) > 60
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 10
        """,
        """
        SELECT customer_name AS name,
               currency_code AS currency,
               ROUND(overdue_60_plus_cents / 100.0, 2) AS owed,
               max_days_overdue
        FROM gold.v_ar_aging
        WHERE COALESCE(overdue_60_plus_cents, 0) > 0
        ORDER BY overdue_60_plus_cents DESC
        LIMIT 10
        """,
    ),
    (
        "Top 10 products by units sold",
        "Bronze: SUM(cantidad * afecta_venta) per id_articulo. Gold: same but via "
        "canonical product. Should match modulo dedup.",
        """
        SELECT TRIM(a.articulo) AS product,
               ROUND(SUM(NULLIF(TRIM(v.cantidad), '')::NUMERIC * NULLIF(TRIM(v.afecta_venta), '')::INT)::NUMERIC, 2) AS units,
               COUNT(DISTINCT v.id_factura) AS invoices
        FROM bronze.ac_ventas v
        JOIN bronze.ac_articulos a ON TRIM(a.id_articulo) = TRIM(v.id_articulo)
        WHERE TRIM(v.empresa) = 'Penicor'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
        """,
        """
        SELECT product_name AS product,
               units_sold AS units,
               invoice_count AS invoices
        FROM gold.v_revenue_by_product
        ORDER BY units_sold DESC
        LIMIT 10
        """,
    ),
]


def fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "Yes" if v else "No"
    if isinstance(v, (Decimal, float)):
        if v == int(v):
            return f"{int(v):,}"
        return f"{v:,.2f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v).strip()


def render_table(rows: list[dict]) -> str:
    if not rows:
        return "*(no rows)*\n"
    cols = list(rows[0].keys())
    out = StringIO()
    out.write("| " + " | ".join(cols) + " |\n")
    out.write("|" + "|".join("---" for _ in cols) + "|\n")
    for r in rows:
        out.write("| " + " | ".join(fmt(r.get(c)) for c in cols) + " |\n")
    return out.getvalue()


def main() -> int:
    load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", "-o", type=Path,
                    default=REPO_ROOT / "reports" / "internal_validation.md")
    args = ap.parse_args()

    db = os.environ.get("PENICOR_DB_URL")
    if not db:
        print("[diag] PENICOR_DB_URL not set", file=sys.stderr)
        return 1

    from sqlalchemy import create_engine, text
    eng = create_engine(db, isolation_level="AUTOCOMMIT")

    out = StringIO()
    out.write("# Internal data validation — Penicor\n\n")
    out.write("Bronze (raw AltoControl copy) compared against Gold (after entity-resolution / signed-aggregation transforms). "
              "If a check is *intended* to differ (dedup), it'll be obvious. If a check should match and doesn't, that's a bug.\n\n")

    with eng.connect() as conn:
        for title, why, bsql, gsql in CHECKS:
            out.write(f"## {title}\n\n{why}\n\n")
            for label, sql in (("Bronze (unaltered)", bsql), ("Gold (transformed)", gsql)):
                out.write(f"**{label}**\n\n")
                try:
                    rows = [dict(r) for r in conn.execute(text(sql)).mappings().all()]
                    out.write(render_table(rows))
                except Exception as e:
                    out.write(f"*ERROR: {e}*\n")
                out.write("\n")
            out.write("\n")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(out.getvalue(), encoding="utf-8")
    print(f"[diag] wrote {args.output.relative_to(REPO_ROOT)}")
    return 0


def load_env() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(REPO_ROOT / ".env")
    except ImportError:
        pass


if __name__ == "__main__":
    sys.exit(main())
