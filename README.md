# text2sql

Give any AI agent a way to talk to your relational databases.

**text2sql** is a Python SDK that translates natural language into SQL using agentic schema retrieval. The agent explores your database schema on its own — searching tables, inspecting columns, testing queries — then returns verified SQL and results. Works with any SQL database (PostgreSQL, MySQL, SQLite, SQL Server, Snowflake, BigQuery) and any LLM provider.

```python
from text2sql import TextSQL

engine = TextSQL("postgresql://user:pass@localhost/mydb")
result = engine.ask("Which customers have spent more than $10K this year?")

print(result.sql)   # verified SQL
print(result.data)  # [{'name': 'Acme Corp', 'total': 14302.50}, ...]
```

## How it works

The text2sql SDK takes full advantage of LLMs' ability to make many tool calls and work in a loop. This architecture lets the agent take as many steps as it needs — more tool calls for harder questions, fewer for simple ones. Here is the current architecture:

```
User: "What's our revenue by product category?"
                    │
                    ▼
        ┌─────────────────────┐
        │   Agent receives    │
        │     question        │
        └────────┬────────────┘
                 │
     ┌───────────▼────────────┐
     │  1. EXPLORE            │  ← queries information_schema / PRAGMA / sys.tables
     │  List all tables       │    to discover what's available
     └───────────┬────────────┘
                 │
     ┌───────────▼────────────┐
     │  2. LOOKUP EXAMPLES    │  ← calls lookup_example("revenue by category")
     │  Check for business    │    gets guidance: which tables, columns, joins
     │  context (optional)    │    to use for this concept
     └───────────┬────────────┘
                 │
     ┌───────────▼────────────┐
     │  3. INSPECT            │  ← PRAGMA table_info('order_items')
     │  Check column names,   │    sees: item_id, order_id, prod_id, qty,
     │  types, keys           │    unit_px_at_sale, discount_pct, line_total
     └───────────┬────────────┘
                 │
     ┌───────────▼────────────┐
     │  4. WRITE & EXECUTE    │  ← runs the SQL, sees the actual results
     │  Test the query        │
     │                        │    ┌─ SQL error? read the error, fix, retry
     │                        │    ├─ Wrong columns? go back to schema
     │                        │    ├─ Results look off? try a different table
     │                        │    └─ Results make sense? done
     └───────────┬────────────┘
                 │ (loop until satisfied)
                 │
     ┌───────────▼────────────┐
     │  5. RETURN              │  ← final verified SQL sent back
     │  Verified SQL           │    middleware re-executes with your row limit
     └────────────────────────┘
```

The agent sees the actual query results at every step. If it writes a query and the output doesn't look right — empty results, unexpected columns, numbers that don't make sense — it goes back to the schema, tries different tables or joins, and re-executes. This self-correction loop runs until the agent is confident the results answer the question.

This is fundamentally different from one-shot RAG, where there's no feedback loop at all. The agent self-corrects at the **retrieval** stage (picked the wrong table? go find the right one), at the **SQL** stage (syntax error? read it and fix), and at the **results** stage (output doesn't match the question? rethink the approach). This is the same architecture used internally at Fidelity, Salesforce, and OpenAI for their production text-to-SQL systems.

### Why this architecture wins

**One-shot RAG fails silently.** If the retriever picks the wrong table, the LLM confidently generates SQL against it. You get a syntactically valid query that returns the wrong answer. There's no feedback loop.

**Agentic retrieval fails loudly.** When the agent picks the wrong table, it executes the query, sees the error or unexpected results, and corrects course. It has the same tools a human analyst would use — browse tables, check columns, test queries, iterate.

This matters most on real-world schemas:
- **50+ tables** where most are irrelevant to any given question
- **Cryptic column names** (`amt_ttl`, `qty_oh`, `unit_px`) that need exploration to understand
- **Multiple valid sources** for the same concept (revenue in `orders`, `payments`, reporting tables)
- **Missing or inconsistent FK naming** (`customer_id` in one table, `cust_id` in another)

## Install

```bash
pip install text2sql

# With Anthropic:
pip install "text2sql[anthropic]"

# With OpenAI:
pip install "text2sql[openai]"
```

## Quick start

```python
from text2sql import TextSQL

# Connect to any SQLAlchemy-supported database
engine = TextSQL("sqlite:///company.db")

# Ask a question
result = engine.ask("Top 5 products by total revenue")
print(result.sql)
print(result.data)

# Control how many rows come back
result = engine.ask("All customers in New York", max_rows=50)
```

## LLM providers

```python
# Anthropic (recommended)
engine = TextSQL("sqlite:///mydb.db", model="anthropic:claude-sonnet-4-6")

# OpenAI
engine = TextSQL("sqlite:///mydb.db", model="openai:gpt-4o")
```

## Database support

Any database with a SQLAlchemy driver:

```python
TextSQL("postgresql://user:pass@localhost/mydb")
TextSQL("mysql+pymysql://user:pass@localhost/mydb")
TextSQL("sqlite:///mydb.db")
TextSQL("mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server")
TextSQL("snowflake://user:pass@account/db/schema")
```

The agent automatically detects the SQL dialect and adjusts its schema exploration strategy — `information_schema` for PostgreSQL/MySQL/Snowflake, `PRAGMA` for SQLite, `sys.tables` for SQL Server.

## Example scenarios

Real databases have jargon, business logic, and naming conventions that no LLM can guess. The `examples` parameter lets you teach the agent your domain:

```markdown
<!-- scenarios.md -->

## net revenue
Net revenue = gross revenue minus refunds.
- `orders.amt_ttl` is the gross order total
- Refunds are in the `payments` table where `is_refund = 1`
- Net = SUM(orders.amt_ttl) + SUM(payments.amt) WHERE is_refund = 1
  (refund amounts are stored as negative values)

## active customers
A customer is "active" if they placed at least one non-cancelled order
in the last 12 months.
```sql
SELECT DISTINCT cust_id FROM orders
WHERE order_date >= DATE('now', '-12 months')
  AND status != 'cancelled'
```

## customer home address
Customer addresses are in `customer_addresses`, NOT on the `customers` table.
- Join on `customers.cust_id = customer_addresses.cust_id`
- Filter: `addr_type = 'billing'` for billing, `addr_type = 'shipping'` for shipping
- `is_default = 1` for the primary address
```

```python
engine = TextSQL(
    "postgresql://localhost/mydb",
    examples="scenarios.md",
)
```

The agent gets a `lookup_example` tool. When a question involves a business concept like "net revenue" or "active customers," the agent calls `lookup_example("net revenue")` and gets your guidance before writing SQL.

### Why examples, not fine-tuning

Fine-tuning bakes knowledge into model weights. When your schema changes, you retrain. When you add a table, you retrain. When a column gets renamed, you retrain.

Examples are a markdown file. Edit it, and the next query uses the updated guidance. No training pipeline, no GPU costs, no deployment. An analyst who knows the schema can write an example in 2 minutes that fixes a class of failures.

## Custom instructions

For global rules that apply to every query:

```python
engine = TextSQL(
    "postgresql://localhost/mydb",
    instructions="""
    - Revenue always means net revenue (after refunds)
    - Always exclude test accounts (email LIKE '%@test.%')
    - Date columns are stored as TEXT in ISO format
    """,
)
```

## Tracing

Every query is traced — which tables the agent explored, what SQL it tried, what errors it hit, how it self-corrected:

```python
engine = TextSQL(
    "sqlite:///mydb.db",
    trace_file="traces.jsonl",  # writes traces to disk
)

result = engine.ask("Top customers by spend")

# See aggregate stats
print(engine.trace_summary())
# {
#   'total_queries': 1,
#   'success_rate': 1.0,
#   'avg_tool_calls_per_query': 4.2,
#   'sql_error_rate': 0.0,
#   ...
# }
```

## text2sql Dashboard (paid)

Writing examples and instructions by hand works, but how do you know *which* examples to write? The [text2sql Dashboard](https://text2sql-dashboard.vercel.app) reads your agent traces, identifies where your LLM is struggling — wrong tables, bad joins, misunderstood business terms — and gives you specific fixes:

- **Auto-populate examples**: the dashboard detects repeated failure patterns (e.g. "the agent keeps using `orders.amt_ttl` when the user asks about net revenue") and generates the exact `scenarios.md` entry to fix it
- **Suggest instruction changes**: when a fix applies to all queries (e.g. "always exclude cancelled orders from revenue"), the dashboard recommends adding it to your global `instructions` instead of a one-off example
- **Track accuracy over time**: see your success rate, error rate, and avg tool calls per query across deployments
- **Run Evals**: 

```python
engine = TextSQL(
    "postgresql://localhost/mydb",
    api_key="t2s_live_abc123...",  # enables auto-sync to dashboard
)
```

Traces sync automatically in the background. Your data never leaves your database — only the agent's tool call metadata (which tables it searched, what SQL it tried, what errors it hit) is sent to the dashboard.

## CLI

```bash
# Interactive mode
text2sql ask "sqlite:///mydb.db"

# Single question
text2sql query "sqlite:///mydb.db" "How many orders per month?"

# With options
text2sql ask "postgresql://localhost/mydb" --model anthropic:claude-sonnet-4-6
```

## Architecture

```
text2sql/
├── core.py          # TextSQL — public API
├── generate.py      # SQLGenerator — builds the agent, parses results
├── connection.py    # Database — SQLAlchemy wrapper
├── tools.py         # execute_sql + lookup_example (LangChain tools)
├── dialects.py      # Per-dialect schema exploration guides
├── examples.py      # ExampleStore — loads scenario markdown
├── tracing.py       # Tracer — captures full agentic loop
├── analyze.py       # AnalysisEngine — deterministic trace analysis
├── models.py        # Pydantic models for analysis reports
└── cli.py           # Click CLI
```

## License

MIT
