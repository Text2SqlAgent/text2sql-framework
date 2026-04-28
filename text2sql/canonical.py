"""Canonical queries — vetted SQL templates that bypass the LLM agent.

For business-critical questions (A/R total, top customer, monthly P&L, etc.)
where determinism, latency, and cost matter more than ad-hoc flexibility,
define a canonical query. When a user's natural-language question matches a
canonical query's name or aliases, the SDK runs the vetted SQL directly
instead of invoking the agent.

File format (same markdown style as scenarios.md):

    ## accounts_receivable_total
    aliases: how much are we owed, total receivables, ar balance, money owed to us
    description: Sum of unpaid invoice amounts.

    ```sql
    SELECT SUM(amount_due) AS total_owed
    FROM invoices
    WHERE status = 'unpaid'
    ```

    ## top_product_last_quarter
    aliases: best selling product last quarter, top product q, top seller last quarter

    ```sql
    SELECT product_name, SUM(quantity) AS units_sold
    FROM order_items
    WHERE order_date >= DATE('now', '-3 months')
    GROUP BY product_name
    ORDER BY units_sold DESC
    LIMIT 1
    ```
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


_STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "of", "in", "on", "at", "to", "for", "from", "by", "with", "as",
    "and", "or", "but", "not", "if", "then", "else", "when", "where",
    "how", "what", "which", "who", "whom", "whose", "why",
    "do", "does", "did", "have", "has", "had", "having",
    "i", "me", "my", "we", "us", "our", "you", "your",
    "this", "that", "these", "those", "there", "here",
    "much", "many", "any", "all", "some", "every", "each",
    "show", "give", "tell", "list", "find", "get", "make", "want",
}

_SQL_BLOCK_RE = re.compile(r"```(?:sql)?\s*\n?(.*?)\n?```", re.DOTALL | re.IGNORECASE)


@dataclass
class CanonicalQuery:
    """A vetted SQL template with natural-language aliases."""

    name: str
    sql: str
    aliases: list[str] = field(default_factory=list)
    description: str = ""

    def all_phrases(self) -> list[str]:
        """All phrases that should match this query (name + aliases)."""
        phrases = [self.name.replace("_", " ")]
        phrases.extend(self.aliases)
        return phrases

    def keyword_set(self) -> set[str]:
        """Tokens drawn from name + aliases, minus stopwords."""
        tokens: set[str] = set()
        for phrase in self.all_phrases():
            for tok in _tokenize(phrase):
                if tok not in _STOPWORDS:
                    tokens.add(tok)
        return tokens


def _tokenize(text: str) -> list[str]:
    # Strip diacritics so accented chars match their unaccented equivalents:
    # "cuánto" -> "cuanto", "más" -> "mas", "año" -> "ano". NFD decomposes
    # "á" into "a" + combining-acute; we drop combining marks. Without this
    # the [a-z0-9]+ regex would split "cuánto" into ["cu", "nto"], breaking
    # canonical matching for any non-English question.
    normalized = unicodedata.normalize("NFD", text.lower())
    ascii_only = "".join(c for c in normalized if not unicodedata.combining(c))
    return re.findall(r"[a-z0-9]+", ascii_only)


@dataclass
class CanonicalMatch:
    """Result of matching a question to a canonical query."""

    query: CanonicalQuery
    score: float
    matched_tokens: set[str]


class CanonicalQueryStore:
    """Loads canonical queries from a markdown file and matches questions to them.

    The matcher is intentionally simple and deterministic: token overlap between
    the question and each canonical query's name + aliases, normalized by the
    smaller of the two token sets. Above a configurable threshold, the question
    is routed to the canonical SQL. Below it, the agent handles the question.
    """

    def __init__(
        self,
        file_path: str | Path,
        match_threshold: float = 0.6,
    ):
        self.file_path = Path(file_path)
        self.queries: list[CanonicalQuery] = []
        self.match_threshold = match_threshold
        self._load()

    def _load(self) -> None:
        if not self.file_path.exists():
            raise FileNotFoundError(f"Canonical queries file not found: {self.file_path}")

        content = self.file_path.read_text(encoding="utf-8")
        # Split on ## headings (top-level only; ```sql blocks may contain headings)
        blocks = re.split(r"(?m)^##\s+", content)
        for block in blocks[1:]:
            self._parse_block(block)

    def _parse_block(self, block: str) -> None:
        lines = block.split("\n", 1)
        name = lines[0].strip().lower()
        body = lines[1] if len(lines) > 1 else ""

        if not name:
            return

        # Extract SQL from the first ```sql code block
        sql_match = _SQL_BLOCK_RE.search(body)
        if not sql_match:
            # Skip entries without SQL
            return
        sql = sql_match.group(1).strip().rstrip(";")

        # Strip the SQL block out before parsing meta lines
        meta_text = _SQL_BLOCK_RE.sub("", body)

        aliases: list[str] = []
        description = ""
        for line in meta_text.split("\n"):
            stripped = line.strip()
            if not stripped:
                continue
            lower = stripped.lower()
            if lower.startswith("aliases:"):
                raw = stripped.split(":", 1)[1]
                aliases = [a.strip().lower() for a in raw.split(",") if a.strip()]
            elif lower.startswith("description:"):
                description = stripped.split(":", 1)[1].strip()

        self.queries.append(
            CanonicalQuery(
                name=name,
                sql=sql,
                aliases=aliases,
                description=description,
            )
        )

    def match(self, question: str) -> Optional[CanonicalMatch]:
        """Return the best canonical match for a question, or None if below threshold."""
        if not self.queries:
            return None

        q_tokens = {t for t in _tokenize(question) if t not in _STOPWORDS}
        if not q_tokens:
            return None

        best: Optional[CanonicalMatch] = None
        for cq in self.queries:
            cq_tokens = cq.keyword_set()
            if not cq_tokens:
                continue
            overlap = q_tokens & cq_tokens
            if not overlap:
                continue
            # Score = overlap / min(|q|, |cq|). Coverage of the smaller set.
            score = len(overlap) / min(len(q_tokens), len(cq_tokens))
            if best is None or score > best.score:
                best = CanonicalMatch(query=cq, score=score, matched_tokens=overlap)

        if best is None or best.score < self.match_threshold:
            return None
        return best

    def list_queries(self) -> list[str]:
        return sorted(q.name for q in self.queries)
