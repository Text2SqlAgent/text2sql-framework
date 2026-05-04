"""AltoControl-specific entity → silver-source mapping.

Customer-namespaced (filename prefix `altocontrol_`). Read by
`etl/entity_resolution.py` to know which silver tables hold which entity
type for AltoControl-based customers (currently just Penicor).

Other customers running on a different ERP get their own
`<customer>_entity_config.py` with the same shape. The generic
entity_resolution module does not import any customer-specific code.
"""

from __future__ import annotations

from etl.entity_resolution import EntitySource


ENTITY_SOURCES: dict[str, EntitySource] = {
    "customer": {
        "table":    "silver.clientes",
        "id_col":   "customer_branch_id",
        # Dedup at the LEGAL-entity grain. trade_name is per-branch
        # ('DEVOTO Nº1 - MALVIN', 'DEVOTO Nº2 - PUNTA GORDA', ...);
        # legal_name is the corporate entity ('DEVOTO HNOS S.A.') shared
        # across branches. Falls back to trade_name when legal_name is NULL.
        "name_col": "COALESCE(legal_name, trade_name)",
    },
    "product": {
        "table":    "silver.articulos",
        "id_col":   "product_id",
        "name_col": "product_name",
    },
    "supplier": {
        "table":    "silver.proveedores",
        "id_col":   "supplier_id",
        # Same legal vs trade dichotomy as customer. Fall back to the
        # display name when razon is missing.
        "name_col": "COALESCE(legal_name, supplier_name)",
    },
    "salesperson": {
        "table":    "silver.vendedores",
        "id_col":   "salesperson_id",
        "name_col": "salesperson_name",
    },
    # warehouse: AltoControl denormalizes warehouse names inside silver.stock
    # rather than carrying a separate dim. Skipped here; the warehouse_master
    # tables in the schema remain available for customers with a real warehouse dim.
}
