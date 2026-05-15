"""Semantic cache layer between canonical queries and the agent path.

When a question doesn't match a canonical template but resembles one we've
already answered ("biggest 10 clients" vs "top 10 customers"), this cache
returns the cached SQL — re-executed against the live DB so data freshness
is preserved. Only the SQL synthesis (the LLM-expensive part) is cached.

Architecture
------------
Position in the dispatch chain:

    user question
         │
         ▼
    canonical match? ──yes──▶ run vetted SQL (~50ms, $0)
         │ no
         ▼
    semantic cache hit? ──yes──▶ run cached SQL (~50ms + embed time, $0)
         │ no
         ▼
    agent path (LLM loop, 5–30s)
         │ on success
         ▼
    store (question, sql) in cache for future

Why this fits between canonical and agent
-----------------------------------------
- Canonicals are deterministic, parameter-free vetted templates. First.
- Semantic cache is for paraphrased repeats of agent-generated SQL we've
  already validated implicitly (it ran successfully once). Second.
- Agent is the fallback for novel questions. Last and most expensive.

The cache stores the SQL TEXT only, not the result rows. We re-execute
on every hit so daily data refreshes are visible. Cross-tenant cache
sharing is unsafe (embedding collisions across schemas) — instances are
per-process and per-database connection by construction.

Embedding backend
-----------------
Default: `sentence-transformers` with `all-MiniLM-L6-v2` (384-dim, ~80MB,
~50ms per encode after model load). Local, free, no API key required.
Install via `text2sql[cache]`. If the dep isn't available, this module
no-ops cleanly — call sites can leave `semantic_cache=None`.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class CacheEntry:
    """A single cached question→SQL mapping."""
    question: str
    embedding: Any              # numpy.ndarray (typed loosely to avoid hard import)
    sql: str
    created_at: float
    hit_count: int = 0


class SemanticCache:
    """In-memory semantic cache for natural-language → SQL pairs.

    Thread-safe (uses an internal RLock). Lookups are O(N) over stored
    entries — fine up to ~10k entries; for larger caches swap in a vector
    index (FAISS, hnswlib).

    Args:
        embedder: callable that takes a string and returns a 1-D numpy array.
                  Required — pass `make_default_embedder()` or your own.
        threshold: minimum cosine similarity to count as a hit. 0.93 is a
                   conservative default — catches paraphrases without
                   merging genuinely different questions.
        ttl_seconds: how long to keep entries. 3600 (1h) by default;
                     short enough to reflect ETL refreshes.
        max_entries: hard cap on cache size. LRU-evicts oldest entries
                     when exceeded.
    """

    def __init__(
        self,
        embedder: Callable[[str], Any],
        threshold: float = 0.93,
        ttl_seconds: int = 3600,
        max_entries: int = 1000,
    ):
        self._embed = embedder
        self.threshold = threshold
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._entries: list[CacheEntry] = []
        self._lock = threading.RLock()
        # Stats
        self.hits = 0
        self.misses = 0
        self.stores = 0

    # -- lookup / store -----------------------------------------------------

    def lookup(self, question: str) -> Optional[CacheEntry]:
        """Return the best matching cache entry above threshold, or None."""
        import numpy as np

        with self._lock:
            self._evict_expired()
            if not self._entries:
                self.misses += 1
                return None

            q_vec = self._embed(question)
            q_norm = q_vec / (np.linalg.norm(q_vec) + 1e-12)

            best_score = -1.0
            best_entry: Optional[CacheEntry] = None
            for entry in self._entries:
                e_norm = entry.embedding / (np.linalg.norm(entry.embedding) + 1e-12)
                score = float(np.dot(q_norm, e_norm))
                if score > best_score:
                    best_score = score
                    best_entry = entry

            if best_entry is None or best_score < self.threshold:
                self.misses += 1
                return None

            best_entry.hit_count += 1
            self.hits += 1
            return best_entry

    def store(self, question: str, sql: str) -> None:
        """Cache a question→SQL pair."""
        if not sql or not sql.strip():
            return
        with self._lock:
            embedding = self._embed(question)
            self._entries.append(CacheEntry(
                question=question,
                embedding=embedding,
                sql=sql,
                created_at=time.time(),
            ))
            self.stores += 1
            self._enforce_max_entries()

    # -- maintenance --------------------------------------------------------

    def _evict_expired(self) -> None:
        """Drop entries older than ttl_seconds. Caller holds the lock."""
        cutoff = time.time() - self.ttl_seconds
        self._entries = [e for e in self._entries if e.created_at >= cutoff]

    def _enforce_max_entries(self) -> None:
        """Drop oldest entries when over capacity. Caller holds the lock."""
        if len(self._entries) <= self.max_entries:
            return
        # Sort by created_at desc, keep newest max_entries
        self._entries.sort(key=lambda e: e.created_at, reverse=True)
        self._entries = self._entries[: self.max_entries]

    def stats(self) -> dict:
        """Snapshot of cache stats. Useful for debugging / observability."""
        with self._lock:
            total = self.hits + self.misses
            return {
                "entries": len(self._entries),
                "hits": self.hits,
                "misses": self.misses,
                "stores": self.stores,
                "hit_rate": self.hits / total if total else 0.0,
            }

    def clear(self) -> None:
        """Drop all cached entries."""
        with self._lock:
            self._entries.clear()


# ---------------------------------------------------------------------------
# Embedder factory
# ---------------------------------------------------------------------------

def make_default_embedder() -> Optional[Callable[[str], Any]]:
    """Build a sentence-transformers backed embedder if available.

    Returns None when the optional dep isn't installed, so callers can
    cleanly skip cache instantiation:

        embedder = make_default_embedder()
        cache = SemanticCache(embedder=embedder) if embedder else None

    The default model is `all-MiniLM-L6-v2`: 384-dim, ~80MB, fast on CPU.
    """
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None

    model = SentenceTransformer("all-MiniLM-L6-v2")

    def _embed(text: str):
        # convert_to_numpy=True returns a plain ndarray (no torch tensor wrapper).
        return model.encode(text, convert_to_numpy=True, show_progress_bar=False)

    return _embed
