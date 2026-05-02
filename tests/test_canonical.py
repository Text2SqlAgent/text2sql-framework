"""Tests for CanonicalQueryStore — matches questions to vetted SQL templates."""

from __future__ import annotations

import pytest

from text2sql.canonical import CanonicalQueryStore


SAMPLE_MD = """
## accounts_receivable_total
aliases: how much are we owed, total receivables, ar balance, money owed to us
description: Sum of unpaid invoice amounts.

```sql
SELECT SUM(amount_due) AS total_owed
FROM invoices
WHERE status = 'unpaid'
```

## top_product_last_quarter
aliases: best selling product last quarter, top seller last quarter

```sql
SELECT product_name, SUM(quantity) AS units_sold
FROM order_items
WHERE order_date >= DATE('now', '-3 months')
GROUP BY product_name
ORDER BY units_sold DESC
LIMIT 1
```

## monthly_expenses
aliases: expense report, expenses by month, monthly spend
description: Total expenses grouped by month.

```sql
SELECT strftime('%Y-%m', expense_date) AS month, SUM(amount) AS total
FROM expenses
GROUP BY month
ORDER BY month DESC
```

## entry_without_sql
This entry has no code block — it should be skipped.
"""


@pytest.fixture
def store(tmp_path):
    md = tmp_path / "canonical.md"
    md.write_text(SAMPLE_MD, encoding="utf-8")
    return CanonicalQueryStore(str(md))


class TestLoading:
    def test_loads_queries_with_sql(self, store):
        names = store.list_queries()
        assert "accounts_receivable_total" in names
        assert "top_product_last_quarter" in names
        assert "monthly_expenses" in names

    def test_skips_entries_without_sql(self, store):
        # entry_without_sql had no ```sql block — should not be loaded
        assert "entry_without_sql" not in store.list_queries()

    def test_aliases_parsed(self, store):
        ar = next(q for q in store.queries if q.name == "accounts_receivable_total")
        assert "how much are we owed" in ar.aliases
        assert "ar balance" in ar.aliases

    def test_description_parsed(self, store):
        ar = next(q for q in store.queries if q.name == "accounts_receivable_total")
        assert "unpaid invoice" in ar.description.lower()

    def test_sql_does_not_include_meta_lines(self, store):
        ar = next(q for q in store.queries if q.name == "accounts_receivable_total")
        assert "aliases:" not in ar.sql.lower()
        assert "description:" not in ar.sql.lower()
        assert ar.sql.strip().startswith("SELECT")

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            CanonicalQueryStore(str(tmp_path / "nope.md"))


class TestMatching:
    def test_alias_phrase_matches(self, store):
        m = store.match("How much are we owed?")
        assert m is not None
        assert m.query.name == "accounts_receivable_total"

    def test_keyword_overlap_matches(self, store):
        m = store.match("show me total receivables please")
        assert m is not None
        assert m.query.name == "accounts_receivable_total"

    def test_top_product_alias(self, store):
        m = store.match("what was the top seller last quarter")
        assert m is not None
        assert m.query.name == "top_product_last_quarter"

    def test_expense_report_alias(self, store):
        m = store.match("generate an expense report")
        assert m is not None
        assert m.query.name == "monthly_expenses"

    def test_unrelated_question_returns_none(self, store):
        m = store.match("what is the weather today")
        assert m is None

    def test_below_threshold_returns_none(self, tmp_path):
        md = tmp_path / "c.md"
        md.write_text(
            "## five_keyword_query\n"
            "aliases: alpha, bravo, charlie, delta, echo\n\n"
            "```sql\nSELECT 1\n```\n",
            encoding="utf-8",
        )
        s = CanonicalQueryStore(str(md), match_threshold=0.8)
        # 5-token question, only 1 token overlaps the canonical's 5 tokens —
        # score = 1/min(5,5) = 0.2, well below the 0.8 threshold.
        assert s.match("alpha foxtrot golf hotel india") is None

    def test_threshold_can_be_lowered(self, tmp_path):
        md = tmp_path / "c.md"
        md.write_text(
            "## ar\naliases: receivables, owed\n\n```sql\nSELECT 1\n```\n",
            encoding="utf-8",
        )
        s = CanonicalQueryStore(str(md), match_threshold=0.4)
        assert s.match("receivables") is not None

    def test_empty_question_returns_none(self, store):
        assert store.match("") is None
        assert store.match("the and of") is None  # all stopwords

    def test_match_includes_score_and_tokens(self, store):
        m = store.match("how much are we owed")
        assert m is not None
        assert 0.0 < m.score <= 1.0
        assert len(m.matched_tokens) > 0
