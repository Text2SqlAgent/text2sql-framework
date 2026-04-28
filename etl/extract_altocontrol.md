# How to run extract_altocontrol.py

Companion to `etl/extract_altocontrol.py` — pulls Penicor's AltoControl
data from Azure SQL into local CSVs for POC development.

## One-time setup

### 1. Check for a SQL Server ODBC driver

The script auto-detects whatever's already installed (Driver 18, 17,
or older fallbacks — Driver 17 is fine for Azure SQL). Check what you
have:

```powershell
Get-OdbcDriver | Where-Object Name -like "*SQL Server*"
```

If you see `ODBC Driver 17 for SQL Server` or `ODBC Driver 18 for SQL Server`
in the list (64-bit), you're good — skip to step 2.

If you don't have any SQL Server ODBC driver, download the MSI from
Microsoft (`https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server`)
and install it. The winget package name varies by Microsoft catalog
version — easier to grab the MSI directly.

### 2. Install Python dependencies (uv)

From the repo root:

```bash
# One-time: create a virtual env (uv handles Python version resolution)
uv venv

# Activate (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Install the ETL extra + your chosen LLM provider
uv pip install -e ".[etl,openrouter]"
```

If you don't have uv: `winget install --id=astral-sh.uv -e` (or `pip install uv`).

The `[etl]` extra brings in `pyodbc`, `pandas`, `psycopg[binary]`, and `python-dotenv` — everything the ETL scripts need. See `pyproject.toml` for the full set of extras.

### 3. Confirm `.env` is in place

The file `text2sql-fork/.env` should already exist with the AltoControl
credentials (created during script setup). It is gitignored — never commit
it. If it's missing, copy `.env.example` to `.env` and fill in:

```
ALTOCONTROL_SERVER=logicoac.database.windows.net
ALTOCONTROL_DATABASE=Penicor
ALTOCONTROL_USER=LoginPenicor
ALTOCONTROL_PASSWORD=<the password>
```

### 4. Bring the WireGuard tunnel up

The Azure SQL firewall whitelists the `br-sao` exit IP. Verify the tunnel
is connected:

```powershell
Get-NetAdapter -Name br-sao
```

Status should be `Up`. If it's not, restart it via the WireGuard GUI app
or:

```powershell
& "C:\Program Files\WireGuard\wireguard.exe" /installtunnelservice "C:\Users\rodri\Downloads\br-sao.conf"
```

(Needs UAC.)

## Run

### Smoke test first (one small SP)

```bash
python etl/extract_altocontrol.py --only clientes
```

Expected output:

```
[extract] connecting to logicoac.database.windows.net / Penicor as LoginPenicor ...
[extract] connected in 1.2s
[extract] clientes                 EXEC dbo.PBI_tabla_clientes  (cap=none)
            ->   1234 rows,  15 cols, 2.1s, db/bronze/altocontrol/clientes.csv

[extract] manifest -> db/bronze/altocontrol/_manifest.json
[extract] total elapsed: 3.3s
```

If this prints, connection + driver + firewall are all working. Continue.

### Full POC pull

```bash
python etl/extract_altocontrol.py
```

This runs all 14 SPs:

- **Capped to 5,000 rows** (recent first): `ventas`, `compras`, `pagos`, `visitas`, `stock`
- **Full pull**: `deuda_por_cliente`, `articulos`, `clientes`, `empresas`, `geografia`, `proveedores`, `ruta`, `rutas`, `vendedores`

Output: 14 CSV files + `_manifest.json` in `db/bronze/altocontrol/`.

### Useful flags

```bash
# Pick specific SPs
python etl/extract_altocontrol.py --only ventas,clientes,articulos

# Different cap (0 = no cap, full pull)
python etl/extract_altocontrol.py --cap 10000
python etl/extract_altocontrol.py --cap 0

# Different output directory
python etl/extract_altocontrol.py --output-dir D:/penicor_dump

# If your installed driver is "17" not "18"
python etl/extract_altocontrol.py --driver "ODBC Driver 17 for SQL Server"

# Longer per-SP timeout
python etl/extract_altocontrol.py --timeout 300
```

## Troubleshooting

**`IM002 — Data source name not found and no default driver specified`**
The auto-detect didn't find any SQL Server ODBC driver. Check
`Get-OdbcDriver` and either install one (any of Driver 17/18 works) or
set `ALTOCONTROL_DRIVER` in `.env` to the exact driver name shown by
`Get-OdbcDriver`.

**`Cannot open server 'logicoac' requested by the login. Client with IP address 'X.X.X.X' is not allowed to access the server`**
Azure SQL firewall blocked your egress IP. Either the `br-sao` tunnel is
down, or Penicor's IT needs to whitelist a new IP. Check the tunnel
first (`Get-NetAdapter -Name br-sao`), then ask IT.

**`Login failed for user 'LoginPenicor'`**
Wrong credentials in `.env`, or the password expired. Double-check
`.env`. The password contains shell-special chars (`):*`) — it's safe
inside `.env` but if you ever paste it into a CLI, quote it.

**A specific SP fails but others succeed**
The script logs the error and keeps going. Check `_manifest.json` —
failed SPs have an `error` field instead of `rows`. Common causes: SP
doesn't exist (typo, or it was renamed), permission denied, or the SP
errors internally on certain inputs.

**The extraction is slow**
Default per-SP timeout is 60s. For very large tables with no cap, raise
with `--timeout 300`. If Azure SQL throttles (DTU exhaustion), wait and
retry.

## What gets produced

```
db/bronze/altocontrol/
├── _manifest.json          ← extraction metadata (timestamps, row counts, columns)
├── ventas.csv              ← 5000 rows of sales (line grain)
├── compras.csv
├── pagos.csv
├── visitas.csv
├── stock.csv
├── articulos.csv           ← full master tables below
├── clientes.csv
├── empresas.csv
├── geografia.csv
├── proveedores.csv
├── ruta.csv
├── rutas.csv
├── vendedores.csv
└── deuda_por_cliente.csv
```

All files are **gitignored** — they contain real customer data and stay
local. Don't commit them.

## What comes next

Once these CSVs exist, the next step is the **bronze loader**: a script
that reads each CSV and inserts into `bronze.ac_<table>` in a local
Postgres. From there: silver transforms, gold facts/dims, then point
text2sql at it. Tell me when the CSVs are ready and I'll scaffold the
loader.

## Re-running

The script overwrites the CSVs on each run. The `_manifest.json` records
the extraction timestamp so you know how stale the data is. Re-run
whenever you want a fresh slice.

When the proper DB dump arrives from Penicor, this script becomes
obsolete — load the dump into local Postgres directly and skip the CSV
intermediate.
