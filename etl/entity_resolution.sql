-- ============================================================================
-- entity_resolution.sql
--
-- Generic entity-resolution registry. Customer-agnostic — applies identically
-- to every customer instance regardless of their source ERP.
--
-- Tables (per entity type: customer, product, supplier, salesperson, warehouse):
--   silver.<entity>_master            canonical entities (the deduped truth)
--   silver.<entity>_aliases           source-system id  ->  canonical_id mapping
--   silver.<entity>_merge_candidates  diagnostic queue of near-dup pairs
--
-- Each <entity>_master carries a canonical_name_norm STORED generated column
-- (UPPER + TRIM + collapsed whitespace) plus a GIN trigram index on it. This
-- is what the pg_trgm fuzzy match operator probes against -- without the
-- indexed normalized column, fuzzy matching falls back to N-squared scans
-- and gets unusable past a few hundred rows per entity.
--
-- Population pipeline (in etl/entity_resolution.py):
--   1. exact_id bootstrap        — every distinct source id gets its own canonical
--   2. fuzzy_auto merge          — collapse near-identical names (sim > 0.95)
--   3. populate_candidates       — log mid-similarity pairs for diagnostics only
--   4. manual overrides          — applied by SQL/API when explicit evidence given
--
-- Default policy: under-merge. Anything below the auto-merge threshold stays
-- separate. We do not proactively merge on guesswork.
--
-- Idempotent.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE SCHEMA IF NOT EXISTS silver;


-- ---------------------------------------------------------------- customer
CREATE TABLE IF NOT EXISTS silver.customer_master (
    canonical_id        BIGSERIAL    PRIMARY KEY,
    canonical_name      TEXT         NOT NULL,
    canonical_name_norm TEXT         GENERATED ALWAYS AS (
                            UPPER(REGEXP_REPLACE(TRIM(canonical_name), '[[:space:]]+', ' ', 'g'))
                        ) STORED,
    legal_name          TEXT,
    tax_id              TEXT,
    entity_kind         TEXT         NOT NULL DEFAULT 'legal_entity'
                                     CHECK (entity_kind IN ('legal_entity', 'group')),
    parent_id           BIGINT       REFERENCES silver.customer_master(canonical_id),
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE silver.customer_master IS
  'Canonical customer entities. One row per real-world customer (after dedup). '
  'entity_kind=group + parent_id form an optional rollup hierarchy. '
  'Default reporting grain is legal_entity.';

CREATE TABLE IF NOT EXISTS silver.customer_aliases (
    source_id      TEXT         PRIMARY KEY,
    canonical_id   BIGINT       NOT NULL REFERENCES silver.customer_master(canonical_id),
    source_name    TEXT,
    match_method   TEXT         NOT NULL CHECK (match_method IN ('exact_id','fuzzy_auto','llm_assisted','manual')),
    confidence     NUMERIC(4,3),
    reviewed_by    TEXT,
    reviewed_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE silver.customer_aliases IS
  'Mapping from source-system customer ids to silver.customer_master.canonical_id. '
  'match_method audits who/what made each merge decision.';

CREATE INDEX IF NOT EXISTS customer_aliases_canonical_idx
    ON silver.customer_aliases (canonical_id);

CREATE INDEX IF NOT EXISTS customer_master_name_norm_trgm_idx
    ON silver.customer_master USING gin (canonical_name_norm gin_trgm_ops);

CREATE INDEX IF NOT EXISTS customer_master_name_norm_btree_idx
    ON silver.customer_master (canonical_name_norm);

CREATE TABLE IF NOT EXISTS silver.customer_merge_candidates (
    id              BIGSERIAL    PRIMARY KEY,
    canonical_id_a  BIGINT       NOT NULL REFERENCES silver.customer_master(canonical_id) ON DELETE CASCADE,
    canonical_id_b  BIGINT       NOT NULL REFERENCES silver.customer_master(canonical_id) ON DELETE CASCADE,
    similarity      NUMERIC(4,3),
    suggested_by    TEXT         NOT NULL,
    status          TEXT         NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending','approved','rejected')),
    decided_by      TEXT,
    decided_at      TIMESTAMPTZ,
    notes           TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (canonical_id_a, canonical_id_b)
);
COMMENT ON TABLE silver.customer_merge_candidates IS
  'Diagnostic: pairs flagged by automated heuristics as possibly the same entity '
  'but below the auto-merge threshold. Audit/observability only.';


-- ---------------------------------------------------------------- product
CREATE TABLE IF NOT EXISTS silver.product_master (
    canonical_id        BIGSERIAL    PRIMARY KEY,
    canonical_name      TEXT         NOT NULL,
    canonical_name_norm TEXT         GENERATED ALWAYS AS (
                            UPPER(REGEXP_REPLACE(TRIM(canonical_name), '[[:space:]]+', ' ', 'g'))
                        ) STORED,
    legal_name          TEXT,
    tax_id              TEXT,
    entity_kind         TEXT         NOT NULL DEFAULT 'legal_entity'
                                     CHECK (entity_kind IN ('legal_entity', 'group')),
    parent_id           BIGINT       REFERENCES silver.product_master(canonical_id),
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.product_aliases (
    source_id      TEXT         PRIMARY KEY,
    canonical_id   BIGINT       NOT NULL REFERENCES silver.product_master(canonical_id),
    source_name    TEXT,
    match_method   TEXT         NOT NULL CHECK (match_method IN ('exact_id','fuzzy_auto','llm_assisted','manual')),
    confidence     NUMERIC(4,3),
    reviewed_by    TEXT,
    reviewed_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS product_aliases_canonical_idx
    ON silver.product_aliases (canonical_id);

CREATE INDEX IF NOT EXISTS product_master_name_norm_trgm_idx
    ON silver.product_master USING gin (canonical_name_norm gin_trgm_ops);

CREATE INDEX IF NOT EXISTS product_master_name_norm_btree_idx
    ON silver.product_master (canonical_name_norm);

CREATE TABLE IF NOT EXISTS silver.product_merge_candidates (
    id              BIGSERIAL    PRIMARY KEY,
    canonical_id_a  BIGINT       NOT NULL REFERENCES silver.product_master(canonical_id) ON DELETE CASCADE,
    canonical_id_b  BIGINT       NOT NULL REFERENCES silver.product_master(canonical_id) ON DELETE CASCADE,
    similarity      NUMERIC(4,3),
    suggested_by    TEXT         NOT NULL,
    status          TEXT         NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending','approved','rejected')),
    decided_by      TEXT,
    decided_at      TIMESTAMPTZ,
    notes           TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (canonical_id_a, canonical_id_b)
);


-- ---------------------------------------------------------------- supplier
CREATE TABLE IF NOT EXISTS silver.supplier_master (
    canonical_id        BIGSERIAL    PRIMARY KEY,
    canonical_name      TEXT         NOT NULL,
    canonical_name_norm TEXT         GENERATED ALWAYS AS (
                            UPPER(REGEXP_REPLACE(TRIM(canonical_name), '[[:space:]]+', ' ', 'g'))
                        ) STORED,
    legal_name          TEXT,
    tax_id              TEXT,
    entity_kind         TEXT         NOT NULL DEFAULT 'legal_entity'
                                     CHECK (entity_kind IN ('legal_entity', 'group')),
    parent_id           BIGINT       REFERENCES silver.supplier_master(canonical_id),
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.supplier_aliases (
    source_id      TEXT         PRIMARY KEY,
    canonical_id   BIGINT       NOT NULL REFERENCES silver.supplier_master(canonical_id),
    source_name    TEXT,
    match_method   TEXT         NOT NULL CHECK (match_method IN ('exact_id','fuzzy_auto','llm_assisted','manual')),
    confidence     NUMERIC(4,3),
    reviewed_by    TEXT,
    reviewed_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS supplier_aliases_canonical_idx
    ON silver.supplier_aliases (canonical_id);

CREATE INDEX IF NOT EXISTS supplier_master_name_norm_trgm_idx
    ON silver.supplier_master USING gin (canonical_name_norm gin_trgm_ops);

CREATE INDEX IF NOT EXISTS supplier_master_name_norm_btree_idx
    ON silver.supplier_master (canonical_name_norm);

CREATE TABLE IF NOT EXISTS silver.supplier_merge_candidates (
    id              BIGSERIAL    PRIMARY KEY,
    canonical_id_a  BIGINT       NOT NULL REFERENCES silver.supplier_master(canonical_id) ON DELETE CASCADE,
    canonical_id_b  BIGINT       NOT NULL REFERENCES silver.supplier_master(canonical_id) ON DELETE CASCADE,
    similarity      NUMERIC(4,3),
    suggested_by    TEXT         NOT NULL,
    status          TEXT         NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending','approved','rejected')),
    decided_by      TEXT,
    decided_at      TIMESTAMPTZ,
    notes           TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (canonical_id_a, canonical_id_b)
);


-- ---------------------------------------------------------------- salesperson
CREATE TABLE IF NOT EXISTS silver.salesperson_master (
    canonical_id        BIGSERIAL    PRIMARY KEY,
    canonical_name      TEXT         NOT NULL,
    canonical_name_norm TEXT         GENERATED ALWAYS AS (
                            UPPER(REGEXP_REPLACE(TRIM(canonical_name), '[[:space:]]+', ' ', 'g'))
                        ) STORED,
    legal_name          TEXT,
    tax_id              TEXT,
    entity_kind         TEXT         NOT NULL DEFAULT 'legal_entity'
                                     CHECK (entity_kind IN ('legal_entity', 'group')),
    parent_id           BIGINT       REFERENCES silver.salesperson_master(canonical_id),
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.salesperson_aliases (
    source_id      TEXT         PRIMARY KEY,
    canonical_id   BIGINT       NOT NULL REFERENCES silver.salesperson_master(canonical_id),
    source_name    TEXT,
    match_method   TEXT         NOT NULL CHECK (match_method IN ('exact_id','fuzzy_auto','llm_assisted','manual')),
    confidence     NUMERIC(4,3),
    reviewed_by    TEXT,
    reviewed_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS salesperson_aliases_canonical_idx
    ON silver.salesperson_aliases (canonical_id);

CREATE INDEX IF NOT EXISTS salesperson_master_name_norm_trgm_idx
    ON silver.salesperson_master USING gin (canonical_name_norm gin_trgm_ops);

CREATE INDEX IF NOT EXISTS salesperson_master_name_norm_btree_idx
    ON silver.salesperson_master (canonical_name_norm);

CREATE TABLE IF NOT EXISTS silver.salesperson_merge_candidates (
    id              BIGSERIAL    PRIMARY KEY,
    canonical_id_a  BIGINT       NOT NULL REFERENCES silver.salesperson_master(canonical_id) ON DELETE CASCADE,
    canonical_id_b  BIGINT       NOT NULL REFERENCES silver.salesperson_master(canonical_id) ON DELETE CASCADE,
    similarity      NUMERIC(4,3),
    suggested_by    TEXT         NOT NULL,
    status          TEXT         NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending','approved','rejected')),
    decided_by      TEXT,
    decided_at      TIMESTAMPTZ,
    notes           TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (canonical_id_a, canonical_id_b)
);


-- ---------------------------------------------------------------- warehouse
CREATE TABLE IF NOT EXISTS silver.warehouse_master (
    canonical_id        BIGSERIAL    PRIMARY KEY,
    canonical_name      TEXT         NOT NULL,
    canonical_name_norm TEXT         GENERATED ALWAYS AS (
                            UPPER(REGEXP_REPLACE(TRIM(canonical_name), '[[:space:]]+', ' ', 'g'))
                        ) STORED,
    legal_name          TEXT,
    tax_id              TEXT,
    entity_kind         TEXT         NOT NULL DEFAULT 'legal_entity'
                                     CHECK (entity_kind IN ('legal_entity', 'group')),
    parent_id           BIGINT       REFERENCES silver.warehouse_master(canonical_id),
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.warehouse_aliases (
    source_id      TEXT         PRIMARY KEY,
    canonical_id   BIGINT       NOT NULL REFERENCES silver.warehouse_master(canonical_id),
    source_name    TEXT,
    match_method   TEXT         NOT NULL CHECK (match_method IN ('exact_id','fuzzy_auto','llm_assisted','manual')),
    confidence     NUMERIC(4,3),
    reviewed_by    TEXT,
    reviewed_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS warehouse_aliases_canonical_idx
    ON silver.warehouse_aliases (canonical_id);

CREATE INDEX IF NOT EXISTS warehouse_master_name_norm_trgm_idx
    ON silver.warehouse_master USING gin (canonical_name_norm gin_trgm_ops);

CREATE INDEX IF NOT EXISTS warehouse_master_name_norm_btree_idx
    ON silver.warehouse_master (canonical_name_norm);

CREATE TABLE IF NOT EXISTS silver.warehouse_merge_candidates (
    id              BIGSERIAL    PRIMARY KEY,
    canonical_id_a  BIGINT       NOT NULL REFERENCES silver.warehouse_master(canonical_id) ON DELETE CASCADE,
    canonical_id_b  BIGINT       NOT NULL REFERENCES silver.warehouse_master(canonical_id) ON DELETE CASCADE,
    similarity      NUMERIC(4,3),
    suggested_by    TEXT         NOT NULL,
    status          TEXT         NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending','approved','rejected')),
    decided_by      TEXT,
    decided_at      TIMESTAMPTZ,
    notes           TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (canonical_id_a, canonical_id_b)
);
