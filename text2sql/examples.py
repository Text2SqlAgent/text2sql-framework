"""Example scenarios loader — reads an MD file of business concept -> SQL guidance mappings."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, List, Tuple


class ExampleStore:
    """
    Loads an MD file where each ## heading is a scenario name
    and the body explains how to query it.

    Example file (examples.md):

        ## customer's home address
        The customer's home address is stored across two tables:
        - `customers.address_line_1`, `customers.address_line_2`
        - `customers.city`, `customers.state`, `customers.zip`
        - For full address with country, join `customers` to `regions` on `region_id`

        ## net revenue
        Net revenue = gross revenue minus refunds.
        - `orders.total_amount` is gross
        - `refunds.amount` is the refund
        - Net = SUM(orders.total_amount) - COALESCE(SUM(refunds.amount), 0)
        - Always join on `orders.order_id = refunds.order_id`

        ## active customers
        A customer is "active" if they have at least one order in the last 12 months.
        ```sql
        SELECT DISTINCT customer_id FROM orders
        WHERE order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH)
        ```
    """

    def __init__(self, file_path):
        # type: (str) -> None
        self.file_path = Path(file_path)
        self.scenarios = {}  # type: dict[str, str]
        self._load()

    def _load(self):
        # type: () -> None
        content = self.file_path.read_text()
        # Split on ## headings
        parts = re.split(r"^##\s+", content, flags=re.MULTILINE)
        for part in parts[1:]:  # skip everything before the first ##
            lines = part.strip().split("\n", 1)
            name = lines[0].strip().lower()
            body = lines[1].strip() if len(lines) > 1 else ""
            self.scenarios[name] = body

    def lookup(self, query):
        # type: (str) -> str
        """Find the best matching scenario for a query string."""
        query_lower = query.lower().strip()

        # Exact match
        if query_lower in self.scenarios:
            return self.scenarios[query_lower]

        # Substring match — find scenarios whose name is contained in the query or vice versa
        best_match = None
        best_score = 0
        for name, body in self.scenarios.items():
            # Check overlap between query words and scenario name words
            query_words = set(query_lower.split())
            name_words = set(name.split())
            overlap = len(query_words & name_words)
            if overlap > best_score:
                best_score = overlap
                best_match = (name, body)

        if best_match and best_score > 0:
            return "## {}\n{}".format(best_match[0], best_match[1])

        return "No matching example found for '{}'. Available scenarios:\n{}".format(
            query, "\n".join("  - {}".format(n) for n in sorted(self.scenarios.keys()))
        )

    def list_scenarios(self):
        # type: () -> List[str]
        """List all available scenario names."""
        return sorted(self.scenarios.keys())
