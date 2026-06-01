"""Build a short PDF for the customer to spot-check data correctness.

Runs a curated set of canonical (deterministic, vetted-SQL) questions against
the live engine and renders the results as compact tables in a single PDF.
The customer reads each block and tells us whether the numbers match their
own books — a fast, low-friction validation pass.

Usage:
    uv run python tools/build_validation_report.py
    uv run python tools/build_validation_report.py --output reports/validacion_penicor.pdf
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

# Each entry: question, validation focus, row cap, columns to suppress.
# `drop_cols` removes columns that aren't useful for client validation —
# e.g. units_sold per customer aggregates across mixed units of measure
# (kg + each), so the number isn't interpretable.
QUESTIONS: list[dict] = [
    {
        "q": "Top 10 clientes por ingresos",
        "focus": ("Confirmar que los 10 clientes principales y sus montos de facturación "
                  "neta coinciden con su tablero interno. Especialmente importante: que "
                  "NO aparezcan duplicados (mismo cliente listado dos veces con nombres "
                  "similares)."),
        "cap": 10,
        "drop_cols": ["units_sold"],  # mixed UoM aggregate — not validatable
    },
    {
        "q": "¿Cuántos clientes tenemos?",
        "focus": ("Verificar el total de clientes únicos (después de unificar duplicados). "
                  "Si el número les parece muy alto o muy bajo, indica un problema en la "
                  "deduplicación."),
        "cap": 5,
        "drop_cols": [],
    },
    {
        "q": "¿Cuánto nos deben?",
        "focus": ("Comparar contra el saldo total de cuentas por cobrar abiertas en su "
                  "sistema, desglosado por moneda."),
        "cap": 5,
        "drop_cols": [],
    },
    {
        "q": "Ventas mensuales",
        "focus": ("Validar la facturación neta mensual del período cargado actualmente "
                  "(extracto inicial: enero–febrero 2026). Buscar saltos extraños o "
                  "totales mensuales que no cuadren con lo esperado."),
        "cap": 12,
        "drop_cols": [],
    },
    {
        "q": "Productos más vendidos",
        "focus": ("Confirmar que los productos top — nombres y unidades vendidas — son "
                  "consistentes con su realidad operativa. Para productos por kilo, "
                  "units_sold representa kilogramos."),
        "cap": 10,
        "drop_cols": [],
    },
    {
        "q": "Clientes más vencidos",
        "focus": ("Validar la lista de clientes con mayor saldo vencido +60 días. "
                  "Verificar que el monto y los días de mora coinciden con lo que "
                  "tienen registrado."),
        "cap": 10,
        "drop_cols": [],
    },
    {
        "q": "Ranking de vendedores",
        "focus": ("Confirmar que la atribución de ventas por vendedor es correcta y "
                  "que ningún vendedor está duplicado o mal asignado."),
        "cap": 10,
        "drop_cols": [],
    },
]


def main() -> int:
    load_env()

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", "-o", type=Path,
                    default=REPO_ROOT / "reports" / "validacion_penicor.pdf",
                    help="Path to write the PDF")
    ap.add_argument("--connection-string", "-c",
                    default=os.environ.get("PENICOR_DB_URL"),
                    help="Postgres URL (default: $PENICOR_DB_URL)")
    ap.add_argument("--canonical", type=Path,
                    default=REPO_ROOT / "examples" / "penicor_canonical.md")
    args = ap.parse_args()

    if not args.connection_string:
        print("[report] PENICOR_DB_URL not set", file=sys.stderr)
        return 1

    from text2sql import TextSQL

    print(f"[report] connecting -> {redact(args.connection_string)}")
    # Canonical-only path: every question in QUESTIONS hits a vetted SQL template,
    # so we never spend tokens or wait on the agent. Model only needs to be valid.
    # Default to OpenRouter (matches whichever key is present in .env). Canonicals
    # short-circuit the model entirely; the model only matters as a fallback if a
    # question's phrasing fails to match any canonical alias.
    if os.environ.get("OPENROUTER_API_KEY"):
        model = "openrouter:anthropic/claude-haiku-4.5"
    elif os.environ.get("ANTHROPIC_API_KEY"):
        model = "anthropic:claude-haiku-4-5"
    else:
        model = "openai:gpt-4o-mini"

    engine = TextSQL(
        connection_string=args.connection_string,
        model=model,
        canonical_queries=str(args.canonical),
    )

    blocks: list[dict] = []
    for i, item in enumerate(QUESTIONS, 1):
        question, focus, cap = item["q"], item["focus"], item["cap"]
        drop_cols = set(item.get("drop_cols") or [])
        print(f"[report] Q{i}: {question}")
        try:
            result = engine.ask(question)
        except Exception as e:
            print(f"        ERROR: {e}", file=sys.stderr)
            blocks.append({
                "question": question, "focus": focus, "error": str(e),
                "rows": [], "row_count": 0, "sql": "",
            })
            continue
        rows = result.data[:cap] if result.data else []
        if drop_cols:
            rows = [{k: v for k, v in r.items() if k not in drop_cols} for r in rows]
        blocks.append({
            "question": question,
            "focus": focus,
            "rows": rows,
            "row_count": len(result.data),
            "shown": len(rows),
            "sql": result.sql,
            "error": None,
        })

    args.output.parent.mkdir(parents=True, exist_ok=True)
    render_pdf(args.output, blocks)
    print(f"[report] wrote {args.output.relative_to(REPO_ROOT)}")
    return 0


def render_pdf(path: Path, blocks: list[dict]) -> None:
    doc = SimpleDocTemplate(
        str(path),
        pagesize=LETTER,
        leftMargin=0.6 * inch,
        rightMargin=0.6 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.6 * inch,
        title="Validación de datos — Penicor",
        author="text2sql",
    )

    base = getSampleStyleSheet()
    h_title = ParagraphStyle(
        "h_title", parent=base["Title"], fontSize=18, leading=22, spaceAfter=6,
    )
    h_intro = ParagraphStyle(
        "h_intro", parent=base["BodyText"], fontSize=10, leading=14, spaceAfter=10,
    )
    h_q = ParagraphStyle(
        "h_q", parent=base["Heading2"], fontSize=13, leading=16,
        textColor=colors.HexColor("#1a3a6c"), spaceBefore=10, spaceAfter=2,
    )
    h_focus = ParagraphStyle(
        "h_focus", parent=base["BodyText"], fontSize=9, leading=12,
        textColor=colors.HexColor("#444444"), spaceAfter=4,
    )
    h_meta = ParagraphStyle(
        "h_meta", parent=base["BodyText"], fontSize=8, leading=10,
        textColor=colors.HexColor("#666666"), spaceAfter=2,
    )
    h_check = ParagraphStyle(
        "h_check", parent=base["BodyText"], fontSize=10, leading=14,
        spaceBefore=4, spaceAfter=8,
    )

    flow = []
    flow.append(Paragraph("Validación de datos — Penicor", h_title))
    flow.append(Paragraph(
        f"Generado el {datetime.now().strftime('%d/%m/%Y')}. "
        "A continuación les presentamos un conjunto de preguntas de negocio "
        "respondidas directamente por el sistema sobre la base de datos consolidada. "
        "El objetivo es que ustedes revisen los resultados y nos confirmen si "
        "los números coinciden con su realidad operativa, o nos indiquen qué "
        "no cuadra. Marquen la casilla correspondiente al final de cada bloque.",
        h_intro,
    ))

    for i, block in enumerate(blocks, 1):
        flow.append(Paragraph(f"{i}. {block['question']}", h_q))
        flow.append(Paragraph(f"<i>Qué validar:</i> {block['focus']}", h_focus))

        if block.get("error"):
            flow.append(Paragraph(
                f"<font color='#b00020'>No se pudo ejecutar: {block['error']}</font>",
                h_focus,
            ))
        elif not block["rows"]:
            flow.append(Paragraph("Sin filas en el resultado.", h_focus))
        else:
            tbl = build_table(block["rows"])
            flow.append(tbl)
            if block["row_count"] > block["shown"]:
                flow.append(Paragraph(
                    f"Mostrando {block['shown']} de {block['row_count']} filas.",
                    h_meta,
                ))

        flow.append(Paragraph(
            "&#9744; Coincide con nuestros datos &nbsp;&nbsp;&nbsp;&nbsp; "
            "&#9744; Algo no cuadra (notas): _______________________________",
            h_check,
        ))

    doc.build(flow)


def _is_empty_signal(v) -> bool:
    """A value carries no information for the customer if it's null, blank,
    or one of the source-side 'no category' sentinels (N/A, S/C, etc.).
    Used to auto-drop columns whose every value is empty so the validation
    PDF doesn't show columns of dashes."""
    if v is None:
        return True
    if isinstance(v, str):
        s = v.strip()
        return s == "" or s.upper() in ("N/A", "S/C", "—", "-", "NA")
    return False


def _useful_columns(rows: list[dict]) -> list[str]:
    """Return columns where at least one row carries a real value."""
    if not rows:
        return []
    keep: list[str] = []
    for col in rows[0].keys():
        if any(not _is_empty_signal(r.get(col)) for r in rows):
            keep.append(col)
    return keep


def build_table(rows: list[dict]) -> Table:
    if not rows:
        return Table([["(sin datos)"]])

    headers = _useful_columns(rows)
    if not headers:
        return Table([["(sin datos significativos)"]])

    body: list[list[str]] = [headers]
    for r in rows:
        body.append([fmt(r.get(h)) for h in headers])

    # Compute approximate column widths based on header length, capped.
    page_w = LETTER[0] - 1.2 * inch
    weights = [max(len(h), 6) for h in headers]
    total = sum(weights)
    col_widths = [page_w * (w / total) for w in weights]

    tbl = Table(body, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a3a6c")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 5),
        ("TOPPADDING", (0, 0), (-1, 0), 5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#f4f6fa")]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cccccc")),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]))
    return tbl


def fmt(v) -> str:
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "Sí" if v else "No"
    if isinstance(v, Decimal):
        return f"{v:,.2f}" if v % 1 else f"{int(v):,}"
    if isinstance(v, float):
        return f"{v:,.2f}" if v != int(v) else f"{int(v):,}"
    if isinstance(v, int):
        return f"{v:,}"
    s = str(v)
    return s if len(s) <= 60 else s[:57] + "..."


def load_env() -> None:
    try:
        from dotenv import load_dotenv
        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass


def redact(url: str) -> str:
    if "://" not in url or "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    creds, host = rest.rsplit("@", 1)
    if ":" not in creds:
        return url
    user, _ = creds.split(":", 1)
    return f"{scheme}://{user}:***@{host}"


if __name__ == "__main__":
    sys.exit(main())
