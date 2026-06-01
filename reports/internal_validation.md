# Internal data validation — Penicor

Bronze (raw AltoControl copy) compared against Gold (after entity-resolution / signed-aggregation transforms). If a check is *intended* to differ (dedup), it'll be obvious. If a check should match and doesn't, that's a bug.

## Top 10 customers by net revenue

Bronze sums raw `neto * afecta_venta` per id_sucursal (i.e., per branch — no dedup). Gold groups by canonical entity. Big chains (Devoto, Disco) should collapse from many branches into one canonical row.

**Bronze (unaltered)**

| name | net_revenue | invoices | branches |
|---|---|---|---|
| DEVOTO HNOS S.A. | 5,600,545.82 | 169 | 23 |
| HENDERSON Y CIA. S.A. | 5,100,080.61 | 17 | 1 |
| Supermercados Disco del Uruguay S.A. | 4,531,255.38 | 162 | 23 |
| INDUNET S.A. | 3,438,385.60 | 15 | 1 |
| INCHE S.A. | 1,045,706.81 | 73 | 2 |
| FABRIX S.A. | 936,602.19 | 23 | 7 |
| SUPERMERCADOS DISCO DEL URUGUAY S.A. | 856,684.39 | 22 | 2 |
| Macromercado Mayorista S.A. | 688,503.45 | 31 | 4 |
| GLACIAL URUGUAY S.R.L | 639,831.91 | 10 | 1 |
| ODALER S.A. | 637,760.19 | 13 | 1 |

**Gold (transformed)**

| name | net_revenue | invoices | branches |
|---|---|---|---|
| DEVOTO HNOS S.A. | 5,600,545.82 | 169 | 23 |
| SUPERMERCADOS DISCO DEL URUGUAY S.A. | 5,387,939.77 | 184 | 25 |
| HENDERSON Y CIA. S.A. | 5,100,080.61 | 17 | 1 |
| INDUNET S.A. | 3,438,385.60 | 15 | 1 |
| MACROMERCADO MAYORISTA S.A. | 1,210,165.30 | 58 | 8 |
| INCHE S.A. | 1,045,706.81 | 73 | 2 |
| FABRIX S.A. | 1,033,462.76 | 37 | 10 |
| GLACIAL URUGUAY S.R.L | 639,831.91 | 10 | 1 |
| ODALER S.A. | 637,760.19 | 13 | 1 |
| CLAUDIA BRACCO | 518,912.32 | 11 | 2 |


## Customer count

Bronze: rows in ac_clientes (one per branch). Gold: deduped legal entities. Big difference is expected.

**Bronze (unaltered)**

| bronze_branch_rows |
|---|
| 2,279 |

**Gold (transformed)**

| gold_canonical_customers |
|---|
| 1,889 |


## Total AR by currency

Bronze: SUM(deuda * 100) on rows where deuda > 0. Gold: same but via the view. These should match exactly — AR isn't deduped at line level.

**Bronze (unaltered)**

| currency | owed | docs |
|---|---|---|
| UYU | 30,388,377.53 | 1,473 |
| USD | 4,803,727.48 | 20 |

**Gold (transformed)**

| currency | owed | docs |
|---|---|---|
| UYU | 30,388,377.53 | 1,473 |
| USD | 4,803,727.48 | 20 |


## Monthly revenue (UYU only)

Both should agree — we only dedup customers/products, not lines. If totals diverge there's a sign or filter bug.

**Bronze (unaltered)**

| month | net_revenue | invoices |
|---|---|---|
| 2026-02 | 14,081,994.80 | 718 |
| 2026-01 | 23,673,565.34 | 997 |

**Gold (transformed)**

| month | net_revenue | invoices |
|---|---|---|
| 2026-02 | 14,081,994.80 | 718 |
| 2026-01 | 23,673,565.34 | 997 |


## Top 10 most overdue customers

Bronze: raw oldest unpaid invoices per branch. Gold: rolled up to canonical customer. The top USD line (GIBUR S.A.) should appear in both.

**Bronze (unaltered)**

| name | currency | owed | max_days_overdue |
|---|---|---|---|
| GIBUR S.A. | USD | 4,023,724.58 | 776 |
| MAXIMILIANO MORA SRL | UYU | 562,063 | 167 |
| OPERADORA DP URUGUAY SA | UYU | 80,095 | 174 |
| RUBEN DEL CAMPO | UYU | 79,470 | 741 |
| FRIGUS SAS | UYU | 44,641 | 65 |
| ELIAS LUIS SANCHEZ MESA | UYU | 30,303 | 181 |
| GIBUR S.A. | UYU | 27,997 | 617 |
| LA CABAÑA S.R.L. | UYU | 22,798 | 92 |
| EDGAR NOEL AMORIN CABRERA | UYU | 22,570 | 207 |
| CATSOLI SAS | UYU | 18,278 | 426 |

**Gold (transformed)**

| name | currency | owed | max_days_overdue |
|---|---|---|---|
| GIBUR S.A. | USD | 4,023,724.58 | 776 |
| MAXIMILIANO MORA SRL | UYU | 562,063 | 167 |
| OPERADORA DP URUGUAY SA | UYU | 80,095 | 174 |
| RUBEN DEL CAMPO | UYU | 79,470 | 741 |
| FRIGUS SAS | UYU | 44,641 | 65 |
| ELIAS LUIS SANCHEZ MESA | UYU | 30,303 | 181 |
| GIBUR S.A. | UYU | 27,997 | 617 |
| LA CABAÑA S.R.L. | UYU | 22,798 | 92 |
| EDGAR NOEL AMORIN CABRERA | UYU | 22,570 | 207 |
| CATSOLI SAS | UYU | 18,278 | 426 |


## Top 10 products by units sold

Bronze: SUM(cantidad * afecta_venta) per id_articulo. Gold: same but via canonical product. Should match modulo dedup.

**Bronze (unaltered)**

| product | units | invoices |
|---|---|---|
| Milanesa de Pollo - Kg | 26,572.50 | 316 |
| Filet entero Gril - Kg | 20,400 | 375 |
| Cubos de Pollo  Kg | 15,070 | 309 |
| BURRITOS POLLO Y QUESO x Ud 165g | 8,489 | 262 |
| BURRITOS CARNE Y QUESO x Ud. 165g | 8,313 | 249 |
| Medallones de Pollo - Kg | 8,092.50 | 154 |
| Tirabuzón x 500 grs. | 7,281 | 4 |
| Spaghetti x 500 grs. | 6,744 | 5 |
| BURRITOS JAMÓN Y QUESO x Ud 165g | 6,547 | 227 |
| (924)Carne picada vacuna Santa Clara- paq. 500 grs | 5,649 | 74 |

**Gold (transformed)**

| product | units | invoices |
|---|---|---|
| Milanesa de Pollo - Kg | 26,572.50 | 316 |
| Filet entero Gril - Kg | 20,400 | 375 |
| Cubos de Pollo  Kg | 15,070 | 309 |
| BURRITOS POLLO Y QUESO x Ud 165g | 8,489 | 262 |
| BURRITOS CARNE Y QUESO x Ud. 165g | 8,313 | 249 |
| Medallones de Pollo - Kg | 8,092.50 | 154 |
| Tirabuzón x 500 grs. | 7,281 | 4 |
| Spaghetti x 500 grs. | 6,744 | 5 |
| BURRITOS JAMÓN Y QUESO x Ud 165g | 6,547 | 227 |
| (924)Carne picada vacuna Santa Clara- paq. 500 grs | 5,649 | 74 |


