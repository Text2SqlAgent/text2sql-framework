-- bronze.ac_*  — raw landing tables for AltoControl ERP extracts.
--
-- All columns are TEXT: bronze is schema-on-read, no transforms. Type
-- coercion + TRIM happen in silver. Audit columns track when a row was
-- ingested and which batch produced it.
--
-- Idempotent: CREATE IF NOT EXISTS so re-running is safe. The loader
-- TRUNCATEs and reloads from CSVs.

CREATE SCHEMA IF NOT EXISTS bronze;

-- =========================================================================
-- Transactions
-- =========================================================================

CREATE TABLE IF NOT EXISTS bronze.ac_ventas (
    id_factura       TEXT,
    id_renglon       TEXT,
    empresa          TEXT,
    fecha            TEXT,
    documento        TEXT,
    id_vendedor      TEXT,
    moneda           TEXT,
    cotizacion       TEXT,
    lista            TEXT,
    id_cliente       TEXT,
    id_sucursal      TEXT,
    id_articulo      TEXT,
    afecta_venta     TEXT,
    valor_medida     TEXT,
    volumen          TEXT,
    cantidad         TEXT,
    cajones          TEXT,
    preciolista      TEXT,
    preciounitario   TEXT,
    subtotal         TEXT,
    descuentos       TEXT,
    neto             TEXT,
    impuestos        TEXT,
    total            TEXT,
    -- audit
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_ventas IS
  'Sales line items from AltoControl PBI_tabla_ventas. Grain: one row per id_renglon. '
  'Sales + credit notes mixed; afecta_venta carries the sign multiplier.';

CREATE TABLE IF NOT EXISTS bronze.ac_compras (
    id_factura       TEXT,
    id_renglon       TEXT,
    empresa          TEXT,
    fecha            TEXT,
    documento        TEXT,
    id_vendedor      TEXT,
    moneda           TEXT,
    cotizacion       TEXT,
    lista            TEXT,
    id_proveedor     TEXT,
    id_articulo      TEXT,
    afecta_venta     TEXT,
    valor_medida     TEXT,
    volumen          TEXT,
    cantidad         TEXT,
    preciolista      TEXT,
    preciounitario   TEXT,
    subtotal         TEXT,
    descuentos       TEXT,
    neto             TEXT,
    impuestos        TEXT,
    total            TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_compras IS
  'Purchase line items from AltoControl PBI_tabla_compras. Grain: one row per id_renglon. '
  'Mirrors ac_ventas but keyed to suppliers (id_proveedor) instead of customers; '
  'no cajones column (purchases are by unit, not by case).';

CREATE TABLE IF NOT EXISTS bronze.ac_pagos (
    id_pago          TEXT,
    empresa          TEXT,
    fecha            TEXT,
    documento        TEXT,
    id_vendedor      TEXT,
    moneda           TEXT,
    cotizacion       TEXT,
    id_cliente       TEXT,
    id_sucursal      TEXT,
    afectacion       TEXT,
    pagos            TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_pagos IS
  'Customer payments / AR collections from AltoControl PBI_tabla_pagos. '
  'pagos column = amount received.';

CREATE TABLE IF NOT EXISTS bronze.ac_visitas (
    id_visita        TEXT,
    fecha            TEXT,
    fecha_inicio     TEXT,
    fecha_fin        TEXT,
    id_vendedor      TEXT,
    id_cliente       TEXT,
    id_sucursal      TEXT,
    id_tipo_visita   TEXT,
    tipo_visita      TEXT,
    observacion      TEXT,
    duracion_min     TEXT,
    duracion_hs      TEXT,
    Latitud          TEXT,
    Longitud         TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_visitas IS
  'Sales-rep visits to customers from AltoControl PBI_tabla_visitas. Geo-tagged. '
  'May be empty if Penicor does not use the visits feature (verify).';

CREATE TABLE IF NOT EXISTS bronze.ac_stock (
    id_deposito      TEXT,
    "Deposito"       TEXT,
    id_articulo      TEXT,
    "Articulo"       TEXT,
    "Cantidad"       TEXT,
    "Cantidad_min"   TEXT,
    "Con_Faltante"   TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_stock IS
  'Inventory snapshot from AltoControl PBI_tabla_stock. Grain: one row per '
  '(deposito, articulo). Already denormalized (deposito + articulo names included). '
  'Con_Faltante flags items below their minimum threshold.';

CREATE TABLE IF NOT EXISTS bronze.ac_deuda_por_cliente (
    id_sucursal      TEXT,
    id_documento     TEXT,
    fecha            TEXT,
    vencimiento      TEXT,
    empresa          TEXT,
    documento        TEXT,
    documento_cfe    TEXT,
    id_vendedor      TEXT,
    moneda           TEXT,
    cotiza           TEXT,
    importe          TEXT,
    deuda            TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_deuda_por_cliente IS
  'Open AR per customer-document from AltoControl PBI_tabla_deuda_por_cliente. '
  'importe = original amount; deuda = outstanding balance; vencimiento = due date. '
  'documento_cfe = Uruguayan electronic fiscal document number.';

-- =========================================================================
-- Master / dimension data
-- =========================================================================

CREATE TABLE IF NOT EXISTS bronze.ac_articulos (
    id_articulo      TEXT,
    articulo         TEXT,
    articulo_cod     TEXT,
    linea            TEXT,
    activo           TEXT,
    familia          TEXT,
    marca            TEXT,
    proveedor        TEXT,
    empresa          TEXT,
    impuesto         TEXT,
    medida           TEXT,
    volumen          TEXT,
    unidad_medida    TEXT,
    "tamaño"         TEXT,
    categoria1       TEXT,
    categoria1_1     TEXT,
    categoria2       TEXT,
    categoria2_1     TEXT,
    categoria3       TEXT,
    categoria3_1     TEXT,
    categoria3_2     TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_articulos IS
  'Product/article master from AltoControl PBI_tabla_articulos. '
  'Hierarchical taxonomy via categoria1..categoria3_2 (6 levels).';

CREATE TABLE IF NOT EXISTS bronze.ac_clientes (
    id_sucursal       TEXT,
    cliente           TEXT,
    sucursal          TEXT,
    nombre            TEXT,
    razon             TEXT,
    razon_cliente     TEXT,
    nombre_cliente    TEXT,
    direccion         TEXT,
    id_pais           TEXT,
    pais              TEXT,
    departamento      TEXT,
    barrio            TEXT,
    lista             TEXT,
    zonaventa         TEXT,
    zonareparto       TEXT,
    categoria1        TEXT,
    categoria1_1      TEXT,
    categoria2        TEXT,
    categoria2_1      TEXT,
    estado            TEXT,
    latitud           TEXT,
    longitud          TEXT,
    rut               TEXT,
    codigo_tipocliente TEXT,
    tipo_cliente      TEXT,
    es_prospect       TEXT,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id      TEXT        NOT NULL,
    source_system     TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id  TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_clientes IS
  'Customer master from AltoControl PBI_tabla_clientes. Natural key is '
  'id_sucursal (format <cliente>-<sucursal>) — one row per customer-branch combo. '
  'rut = Uruguayan tax ID. zonaventa vs zonareparto = sales vs delivery zones.';

CREATE TABLE IF NOT EXISTS bronze.ac_empresas (
    id_empresa       TEXT,
    empresa          TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_empresas IS
  'Legal entities sharing this AltoControl instance: Penicor + 3 others. '
  'Project scope is Penicor only — silver/gold filter on TRIM(empresa) = ''Penicor''.';

CREATE TABLE IF NOT EXISTS bronze.ac_geografia (
    id_pais          TEXT,
    pais             TEXT,
    id_departamento  TEXT,
    departamento     TEXT,
    departamento_pais TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_geografia IS
  'Country + department reference data from AltoControl PBI_tabla_geografia. '
  'Uruguay has 19 departments; row count of ~21 reflects UY + neighbors.';

CREATE TABLE IF NOT EXISTS bronze.ac_proveedores (
    id_proveedor     TEXT,
    nombre           TEXT,
    razon            TEXT,
    rut              TEXT,
    direccion        TEXT,
    lista            TEXT,
    categoria        TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_proveedores IS
  'Supplier master from AltoControl PBI_tabla_proveedores.';

CREATE TABLE IF NOT EXISTS bronze.ac_ruta (
    id_ruta          TEXT,
    ruta             TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_ruta IS
  'Distribution-route master from AltoControl PBI_tabla_ruta. ~7 routes.';

CREATE TABLE IF NOT EXISTS bronze.ac_rutas (
    vendedor         TEXT,
    id_ruta          TEXT,
    ruta             TEXT,
    cliente          TEXT,
    sucursal         TEXT,
    orden            TEXT,
    id_sucursal      TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_rutas IS
  'Route assignments from AltoControl PBI_tabla_rutas. '
  'Many-to-many: which sales rep visits which customer branch on which route, in what order.';

CREATE TABLE IF NOT EXISTS bronze.ac_vendedores (
    id_vendedor      TEXT,
    vendedor         TEXT,
    serie            TEXT,
    deposito         TEXT,
    supervisor       TEXT,
    empresa          TEXT,
    zona_reparto     TEXT,
    zona_venta       TEXT,
    tipo_vendedor    TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_batch_id     TEXT        NOT NULL,
    source_system    TEXT        NOT NULL DEFAULT 'altocontrol',
    source_record_id TEXT        NOT NULL
);
COMMENT ON TABLE bronze.ac_vendedores IS
  'Salesperson master from AltoControl PBI_tabla_vendedores. '
  'serie = invoice number series assigned to this rep. supervisor = hierarchical reporting.';

-- Indexes on ingested_at for incremental queries (silver loader uses these
-- to find rows newer than the last successful silver load).
CREATE INDEX IF NOT EXISTS ac_ventas_ingested_at_idx            ON bronze.ac_ventas (ingested_at);
CREATE INDEX IF NOT EXISTS ac_compras_ingested_at_idx           ON bronze.ac_compras (ingested_at);
CREATE INDEX IF NOT EXISTS ac_pagos_ingested_at_idx             ON bronze.ac_pagos (ingested_at);
CREATE INDEX IF NOT EXISTS ac_visitas_ingested_at_idx           ON bronze.ac_visitas (ingested_at);
CREATE INDEX IF NOT EXISTS ac_stock_ingested_at_idx             ON bronze.ac_stock (ingested_at);
CREATE INDEX IF NOT EXISTS ac_deuda_por_cliente_ingested_at_idx ON bronze.ac_deuda_por_cliente (ingested_at);
