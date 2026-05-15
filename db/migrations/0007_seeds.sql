-- ============================================================================
-- 0007_seeds.sql
--
-- Reference-data seeds:
--   * dim_currency: common ISO 4217 codes.
--   * dim_date: 5 years backward + 5 years forward from today, with
--     fiscal_year/fiscal_quarter/fiscal_month derived from
--     tenant_config.fiscal_year_start_month.
--
-- Idempotent: ON CONFLICT DO NOTHING for currency; dim_date uses
-- INSERT ... ON CONFLICT (date_key) DO NOTHING after generate_series.
--
-- IMPORTANT: 0002_tenant_config.sql does NOT insert a row. The operator
-- must INSERT INTO gold.tenant_config (...) VALUES (...) BEFORE running
-- this migration, because dim_date math depends on it.
-- ============================================================================


-- ---------- Guard: tenant_config must exist with one row ------------------

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM gold.tenant_config) THEN
    RAISE EXCEPTION
      'gold.tenant_config has no row. Insert the tenant config row before running 0007_seeds.sql. '
      'Example: INSERT INTO gold.tenant_config (customer_legal_name, customer_short_name, base_currency_code, fiscal_year_start_month, reporting_timezone) VALUES (...);';
  END IF;
END $$;


-- ---------- dim_currency (common codes) -----------------------------------

INSERT INTO gold.dim_currency (currency_code, currency_name, symbol, decimal_places)
VALUES
  ('USD','US Dollar','$',2),
  ('EUR','Euro','€',2),
  ('GBP','British Pound','£',2),
  ('JPY','Japanese Yen','¥',0),
  ('CNY','Chinese Yuan','¥',2),
  ('CAD','Canadian Dollar','$',2),
  ('AUD','Australian Dollar','$',2),
  ('MXN','Mexican Peso','$',2),
  ('BRL','Brazilian Real','R$',2),
  ('PEN','Peruvian Sol','S/',2),
  ('CLP','Chilean Peso','$',0),
  ('COP','Colombian Peso','$',2),
  ('ARS','Argentine Peso','$',2),
  ('CHF','Swiss Franc','Fr',2),
  ('SEK','Swedish Krona','kr',2),
  ('NOK','Norwegian Krone','kr',2),
  ('DKK','Danish Krone','kr',2),
  ('INR','Indian Rupee','₹',2),
  ('SGD','Singapore Dollar','$',2),
  ('HKD','Hong Kong Dollar','$',2),
  ('NZD','New Zealand Dollar','$',2),
  ('ZAR','South African Rand','R',2),
  ('KRW','South Korean Won','₩',0),
  ('TRY','Turkish Lira','₺',2),
  ('AED','UAE Dirham','د.إ',2)
ON CONFLICT (currency_code) DO NOTHING;


-- ---------- dim_date (5y back + 5y forward) -------------------------------

DO $$
DECLARE
  fy_start_month SMALLINT;
  start_date DATE;
  end_date   DATE;
BEGIN
  SELECT fiscal_year_start_month INTO fy_start_month FROM gold.tenant_config LIMIT 1;

  start_date := (CURRENT_DATE - INTERVAL '5 years')::DATE;
  end_date   := (CURRENT_DATE + INTERVAL '5 years')::DATE;

  INSERT INTO gold.dim_date (
    date_key, day_of_month, day_of_week, day_name,
    iso_week, month_number, month_name, quarter, year,
    fiscal_year, fiscal_quarter, fiscal_month,
    is_weekend, is_holiday
  )
  SELECT
    d::DATE                                         AS date_key,
    EXTRACT(DAY FROM d)::SMALLINT                   AS day_of_month,
    EXTRACT(DOW FROM d)::SMALLINT                   AS day_of_week,
    to_char(d, 'FMDay')                             AS day_name,
    EXTRACT(WEEK FROM d)::SMALLINT                  AS iso_week,
    EXTRACT(MONTH FROM d)::SMALLINT                 AS month_number,
    to_char(d, 'FMMonth')                           AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT               AS quarter,
    EXTRACT(YEAR FROM d)::SMALLINT                  AS year,

    -- Fiscal year: if month >= fy_start_month, fiscal_year = calendar year; else year - 1
    (CASE
       WHEN EXTRACT(MONTH FROM d) >= fy_start_month THEN EXTRACT(YEAR FROM d)
       ELSE EXTRACT(YEAR FROM d) - 1
     END)::SMALLINT                                 AS fiscal_year,

    -- Fiscal month: month-of-fiscal-year, 1..12.
    -- ((calendar_month - fy_start_month) MOD 12) + 1
    ((MOD(EXTRACT(MONTH FROM d)::INT - fy_start_month + 12, 12)) + 1)::SMALLINT
                                                    AS fiscal_month,

    -- Fiscal quarter: ceil(fiscal_month / 3)
    (((MOD(EXTRACT(MONTH FROM d)::INT - fy_start_month + 12, 12)) / 3) + 1)::SMALLINT
                                                    AS fiscal_quarter,

    EXTRACT(DOW FROM d) IN (0, 6)                   AS is_weekend,
    false                                           AS is_holiday

  FROM generate_series(start_date, end_date, INTERVAL '1 day') d
  ON CONFLICT (date_key) DO NOTHING;
END $$;
