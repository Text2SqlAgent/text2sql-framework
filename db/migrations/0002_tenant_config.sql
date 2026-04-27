-- ============================================================================
-- 0002_tenant_config.sql
--
-- Single-row configuration table for the customer instance: fiscal calendar,
-- base currency, timezone, legal name. Referenced by views (e.g. fiscal year
-- math in the date dimension) and read by our service layer to populate
-- TextSQL.metadata_hint at runtime.
--
-- Idempotent.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.tenant_config (
  -- Singleton: enforced via the only_one_row CHECK below.
  id                              BOOLEAN     PRIMARY KEY DEFAULT true,

  customer_legal_name             TEXT        NOT NULL,
  customer_short_name             TEXT        NOT NULL,
  base_currency_code              TEXT        NOT NULL,        -- ISO 4217
  fiscal_year_start_month         SMALLINT    NOT NULL CHECK (fiscal_year_start_month BETWEEN 1 AND 12),
  reporting_timezone              TEXT        NOT NULL,        -- IANA tz, e.g. 'America/Lima'

  onboarded_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
  notes                           TEXT,

  CONSTRAINT only_one_row CHECK (id = true)
);

COMMENT ON TABLE gold.tenant_config IS
  'Singleton configuration row for this customer instance. Read by views '
  'and the service layer; populated during onboarding.';
COMMENT ON COLUMN gold.tenant_config.customer_legal_name IS
  'Customer legal name as it appears on contracts and invoices.';
COMMENT ON COLUMN gold.tenant_config.customer_short_name IS
  'Short display name used in the chat UI.';
COMMENT ON COLUMN gold.tenant_config.base_currency_code IS
  'ISO 4217 currency code. All "in base currency" views convert to this.';
COMMENT ON COLUMN gold.tenant_config.fiscal_year_start_month IS
  'Calendar month (1-12) the customer''s fiscal year starts in. '
  'Drives fiscal_year/fiscal_quarter columns in dim_date.';
COMMENT ON COLUMN gold.tenant_config.reporting_timezone IS
  'IANA timezone (e.g. "America/Lima"). All TIMESTAMPTZ values are stored '
  'in UTC; this column tells the UI/agent how to render local times.';
