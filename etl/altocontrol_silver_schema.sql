-- silver.* — cleaned, typed, deduplicated entities for the AltoControl pipeline.
--
-- Conventions (per IDEAL_DATABASE.md §6 + the demo's silver_schema.sql):
--   - English column names (Spanish stays in bronze; silver is the
--     translation point so the gold/agent layer reads cleanly).
--   - All text fields TRIMmed of the AltoControl CHAR padding upstream.
--   - Money in integer cents (NUMERIC * 100 ::BIGINT).
--   - Dates as DATE; timestamps as TIMESTAMPTZ.
--   - Booleans for "Si"/"No" source values.
--   - Penicor scope: tables that have an `empresa` source column are
--     filtered to TRIM(empresa) = 'Penicor' in the transform; tables that
--     don't (clientes, geografia, proveedores, ruta, rutas, stock,
--     visitas) carry all rows since they're not entity-scoped in source.
--   - "S/C" (sin categoría) and "N/A" left as literal text — they are
--     meaningful "no category" markers, not NULL.
--
-- Idempotent: CREATE IF NOT EXISTS. The transform script TRUNCATEs and
-- reloads from bronze on every run.

CREATE SCHEMA IF NOT EXISTS silver;

-- =========================================================================
-- Reference / master
-- =========================================================================

CREATE TABLE IF NOT EXISTS silver.empresas (
    empresa_id          INT  PRIMARY KEY,
    empresa_name        TEXT
);
COMMENT ON TABLE silver.empresas IS
  'All 4 legal entities sharing the AltoControl instance (Penicor + 3 others). '
  'Kept in full for reference even though the rest of silver is Penicor-scoped.';

CREATE TABLE IF NOT EXISTS silver.geografia (
    country_code        TEXT NOT NULL,
    country_name        TEXT,
    department_id       INT  NOT NULL,
    department_name     TEXT,
    department_country  TEXT,
    PRIMARY KEY (country_code, department_id)
);
COMMENT ON TABLE silver.geografia IS
  'Country + department reference. Uruguay has 19 departments + a few neighbor entries.';

CREATE TABLE IF NOT EXISTS silver.clientes (
    customer_branch_id   TEXT PRIMARY KEY,    -- id_sucursal, e.g. "0-1"
    customer_id          INT  NOT NULL,       -- the cliente part of "<cliente>-<sucursal>"
    branch_id            INT  NOT NULL,       -- the sucursal part
    trade_name           TEXT,                -- nombre / nombre_cliente (consumer-facing)
    legal_name           TEXT,                -- razon / razon_cliente (RUT-registered)
    address              TEXT,
    country_code         TEXT,
    country_name         TEXT,
    department           TEXT,
    district             TEXT,                -- barrio
    price_list           TEXT,
    sales_zone           TEXT,                -- zonaventa
    delivery_zone        TEXT,                -- zonareparto
    category_1           TEXT,
    category_1_1         TEXT,
    category_2           TEXT,
    category_2_1         TEXT,
    status               TEXT,                -- estado (e.g. 'Activo')
    latitude             NUMERIC(10,7),
    longitude            NUMERIC(10,7),
    rut                  TEXT,                -- Uruguayan tax ID
    customer_type_code   INT,
    customer_type        TEXT,
    is_prospect          BOOLEAN NOT NULL DEFAULT false
);
COMMENT ON TABLE silver.clientes IS
  'Customer master. Natural key is customer_branch_id (id_sucursal in source). '
  'One row per customer-branch combo; a single customer with multiple delivery '
  'branches gets multiple rows here.';

CREATE TABLE IF NOT EXISTS silver.articulos (
    product_id           TEXT PRIMARY KEY,    -- id_articulo, e.g. "00001" (leading zeros)
    product_name         TEXT,
    product_code         TEXT,                -- articulo_cod (display code)
    product_line         TEXT,                -- linea
    is_active            BOOLEAN NOT NULL DEFAULT true,
    family               TEXT,                -- familia
    brand                TEXT,                -- marca
    default_supplier     TEXT,                -- proveedor (denormalized name)
    empresa              TEXT,                -- always 'Penicor' after filter
    tax_class            TEXT,                -- impuesto
    measure              NUMERIC(12,3),       -- medida
    volume               NUMERIC(12,3),
    unit_of_measure      TEXT,                -- unidad_medida (e.g. 'EA')
    size                 TEXT,                -- tamaño (free text)
    category_1           TEXT,
    category_1_1         TEXT,
    category_2           TEXT,
    category_2_1         TEXT,
    category_3           TEXT,
    category_3_1         TEXT,
    category_3_2         TEXT
);
COMMENT ON TABLE silver.articulos IS
  'Product master, Penicor-scoped (filtered on empresa). '
  '6-level category hierarchy (categoria1 .. categoria3_2 in source).';

CREATE TABLE IF NOT EXISTS silver.proveedores (
    supplier_id          INT  PRIMARY KEY,
    supplier_name        TEXT,
    legal_name           TEXT,                -- razon
    rut                  TEXT,
    address              TEXT,
    price_list           TEXT,
    category             TEXT
);
COMMENT ON TABLE silver.proveedores IS
  'Supplier master. Not empresa-scoped in source; all rows carried.';

CREATE TABLE IF NOT EXISTS silver.vendedores (
    salesperson_id       INT  PRIMARY KEY,
    salesperson_name     TEXT,
    invoice_series       TEXT,                -- serie (document number series for this rep)
    warehouse_id         INT,                 -- deposito
    supervisor_id        INT,
    empresa              TEXT,                -- always 'Penicor' after filter
    delivery_zone        TEXT,
    sales_zone           TEXT,
    salesperson_type     TEXT
);
COMMENT ON TABLE silver.vendedores IS
  'Salesperson master, Penicor-scoped. invoice_series is the document-number '
  'prefix this rep is authorized to issue.';

CREATE TABLE IF NOT EXISTS silver.routes (
    route_id             INT  PRIMARY KEY,
    route_name           TEXT                 -- e.g. 'Lunes'
);
COMMENT ON TABLE silver.routes IS
  'Distribution-route master. Penicor uses 7 routes named after days of the week.';

CREATE TABLE IF NOT EXISTS silver.route_assignments (
    salesperson_id       INT  NOT NULL,
    route_id             INT  NOT NULL,
    customer_branch_id   TEXT NOT NULL,
    visit_order          INT,                 -- orden (sequence on the route)
    PRIMARY KEY (salesperson_id, route_id, customer_branch_id)
);
COMMENT ON TABLE silver.route_assignments IS
  'Many-to-many: which salesperson visits which customer-branch on which weekday route, '
  'and in what stop sequence.';

-- =========================================================================
-- Transactions
-- =========================================================================

CREATE TABLE IF NOT EXISTS silver.ventas (
    line_id              TEXT PRIMARY KEY,     -- id_renglon
    invoice_id           TEXT NOT NULL,        -- id_factura
    fecha                DATE NOT NULL,
    document_type        TEXT,                 -- documento
    salesperson_id       INT,
    currency_code        TEXT,
    exchange_rate        NUMERIC(18,8),
    price_list           TEXT,
    customer_id          INT,
    customer_branch_id   TEXT,
    product_id           TEXT,
    affects_sale         SMALLINT,             -- afecta_venta: -1 (credit note), 0, 1 (sale)
    measure_value        NUMERIC(12,3),
    volume               NUMERIC(12,3),
    quantity             NUMERIC(12,3),
    cases                NUMERIC(12,3),        -- cajones
    list_price_cents     BIGINT,
    unit_price_cents     BIGINT,
    subtotal_cents       BIGINT,
    discount_cents       BIGINT,
    net_cents            BIGINT,
    tax_cents            BIGINT,
    total_cents          BIGINT
);
COMMENT ON TABLE silver.ventas IS
  'Sales line items, Penicor-scoped. Sales + credit notes mixed (sign in '
  'affects_sale). Money columns are integer cents in the line currency.';

CREATE TABLE IF NOT EXISTS silver.compras (
    line_id              TEXT PRIMARY KEY,
    invoice_id           TEXT NOT NULL,
    fecha                DATE NOT NULL,
    document_type        TEXT,
    salesperson_id       INT,
    currency_code        TEXT,
    exchange_rate        NUMERIC(18,8),
    price_list           TEXT,
    supplier_id          INT,
    product_id           TEXT,
    affects_sale         SMALLINT,
    measure_value        NUMERIC(12,3),
    volume               NUMERIC(12,3),
    quantity             NUMERIC(12,3),
    list_price_cents     BIGINT,
    unit_price_cents     BIGINT,
    subtotal_cents       BIGINT,
    discount_cents       BIGINT,
    net_cents            BIGINT,
    tax_cents            BIGINT,
    total_cents          BIGINT
);
COMMENT ON TABLE silver.compras IS
  'Purchase line items, Penicor-scoped. No "cases" column (purchases are by unit). '
  'Otherwise mirrors ventas, with id_proveedor instead of id_cliente.';

CREATE TABLE IF NOT EXISTS silver.pagos (
    payment_id           TEXT PRIMARY KEY,     -- id_pago
    fecha                DATE NOT NULL,
    document_type        TEXT,
    salesperson_id       INT,
    currency_code        TEXT,
    exchange_rate        NUMERIC(18,8),
    customer_id          INT,
    customer_branch_id   TEXT,
    affects_ar           SMALLINT,             -- afectacion (sign for AR impact)
    amount_cents         BIGINT
);
COMMENT ON TABLE silver.pagos IS
  'Customer payments / AR collections, Penicor-scoped.';

CREATE TABLE IF NOT EXISTS silver.deuda_por_cliente (
    customer_branch_id   TEXT NOT NULL,
    document_id          TEXT NOT NULL,
    document_date        DATE,
    due_date             DATE,
    document_type        TEXT,
    cfe_document         TEXT,                 -- documento_cfe (Uruguayan e-invoice)
    salesperson_id       INT,
    currency_code        TEXT,
    exchange_rate        NUMERIC(18,8),
    amount_cents         BIGINT,               -- importe (original)
    outstanding_cents    BIGINT,               -- deuda (balance still owed)
    PRIMARY KEY (customer_branch_id, document_id)
);
COMMENT ON TABLE silver.deuda_por_cliente IS
  'Open AR (one row per outstanding document), Penicor-scoped. '
  'Backs the "how much are we owed" canonical query family.';

CREATE TABLE IF NOT EXISTS silver.visitas (
    visit_id             TEXT PRIMARY KEY,     -- id_visita
    fecha                DATE,
    start_at             TIMESTAMPTZ,
    end_at               TIMESTAMPTZ,
    salesperson_id       INT,
    customer_id          INT,
    customer_branch_id   TEXT,
    visit_type_id        INT,
    visit_type           TEXT,
    notes                TEXT,                 -- observacion
    duration_minutes     NUMERIC(10,2),
    duration_hours       NUMERIC(10,2),
    latitude             NUMERIC(10,7),
    longitude            NUMERIC(10,7)
);
COMMENT ON TABLE silver.visitas IS
  'Sales-rep visits to customer branches. Geo-tagged. May be empty in source.';

CREATE TABLE IF NOT EXISTS silver.stock (
    warehouse_id         INT  NOT NULL,
    warehouse_name       TEXT,
    product_id           TEXT NOT NULL,
    product_name         TEXT,
    quantity             NUMERIC(12,3),
    min_quantity         NUMERIC(12,3) DEFAULT 0,
    is_understocked      BOOLEAN,             -- Con_Faltante = quantity < min_quantity
    PRIMARY KEY (warehouse_id, product_id)
);
COMMENT ON TABLE silver.stock IS
  'Inventory snapshot, denormalized (warehouse + product names included). '
  'is_understocked flags items below their per-warehouse minimum threshold.';
