-- ============================================================================
-- 0001_schemas_and_roles.sql
--
-- Bootstrap a fresh customer Postgres database with the medallion-layer
-- schemas (bronze / silver / gold) and the three operational roles described
-- in IDEAL_DATABASE.md §3 and §9.
--
-- Run as a superuser (the `postgres` user, or an RDS master user).
-- Idempotent: safe to re-run.
-- ============================================================================

-- ---------- Schemas ---------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS bronze;
COMMENT ON SCHEMA bronze IS
  'Raw landing zone — one table per source system per object. Append-only. '
  'No transformations. Not exposed to the agent.';

CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS
  'Cleaned, deduped, type-cast, conformed-keys layer. One row per logical '
  'entity. Used internally; not exposed to the agent.';

CREATE SCHEMA IF NOT EXISTS gold;
COMMENT ON SCHEMA gold IS
  'Semantic layer — fact/dim tables and curated views. The ONLY schema '
  'exposed to the text2sql read-only agent role.';


-- ---------- Roles -----------------------------------------------------------
-- We create roles only if they don't already exist. Passwords are NOT set
-- here — the operator sets them via ALTER ROLE after running the pack.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_writer') THEN
    CREATE ROLE etl_writer NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_admin') THEN
    CREATE ROLE service_admin NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agent_reader') THEN
    CREATE ROLE agent_reader NOLOGIN;
  END IF;
END $$;

COMMENT ON ROLE etl_writer    IS 'Write access to bronze/silver/gold for ingestion workers.';
COMMENT ON ROLE service_admin IS 'Read-only across all schemas, for our internal ops dashboards.';
COMMENT ON ROLE agent_reader  IS 'Read-only on gold.* only — the chat UI / text2sql connection.';


-- ---------- Schema-level USAGE grants --------------------------------------

GRANT USAGE ON SCHEMA bronze, silver, gold TO etl_writer, service_admin;
GRANT USAGE ON SCHEMA gold                  TO agent_reader;

-- ETL needs DDL on bronze and CRUD on all three layers.
GRANT CREATE ON SCHEMA bronze, silver, gold TO etl_writer;


-- ---------- Default privileges for objects created LATER -------------------
-- These apply to tables created in subsequent migrations and by ETL.

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO service_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO service_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT SELECT ON TABLES TO service_admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT SELECT ON TABLES TO agent_reader;

-- Same for sequences (BIGSERIAL).
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT USAGE, SELECT ON SEQUENCES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT USAGE, SELECT ON SEQUENCES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT USAGE, SELECT ON SEQUENCES TO etl_writer;


-- ---------- Lock down PUBLIC -----------------------------------------------
-- By default in stock Postgres, any logged-in role can CREATE in PUBLIC
-- and SELECT from public.*. We don't use PUBLIC; revoke to keep the
-- database surface tight.

REVOKE ALL ON SCHEMA public FROM PUBLIC;


-- ---------- Search path defaults -------------------------------------------
-- Anyone who logs in as agent_reader gets `gold` first on their search_path,
-- so canonical SQL like "SELECT ... FROM v_invoices" resolves without
-- schema-qualifying every table.

-- NOTE: the application connection string can also pass
--   ?options=-csearch_path%3Dgold
-- which is more robust than relying on the role default. We set both.

DO $$
BEGIN
  EXECUTE format(
    'ALTER ROLE agent_reader IN DATABASE %I SET search_path TO gold',
    current_database()
  );
END $$;


-- Quick sanity row to log this migration ran. Audited via pg_class queries
-- if needed. (Optional — no migration history table here; use a tool like
-- sqitch or flyway later if you want one.)
