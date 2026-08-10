# text2sql-mcp

<!-- mcp-name: io.github.cpenniman12/text2sql-mcp -->

A keyless, persistent Text2SQL workspace for Claude Code and other MCP coding
assistants. The coding assistant supplies the model reasoning; this server
supplies restricted Python, read-only database access, verification, and traces.

## Claude Code setup

Install the framework, then scaffold the project:

```bash
pip install text2sql-framework
cd your-project
text2sql init --database-type postgres
export TEXT2SQL_DATABASE_URL='postgresql://readonly@localhost/analytics'
claude
```

`text2sql init` creates `.mcp.json` and
`.claude/agents/text2sql.md`. It does not store the database URL or any model API
key. Ask Claude to "use the text2sql subagent" for a database question.

Manual MCP configuration:

```json
{
  "mcpServers": {
    "text2sql": {
      "command": "uvx",
      "args": ["--from", "text2sql-mcp>=0.2.0", "text2sql-mcp"],
      "env": {
        "TEXT2SQL_DATABASE_URL": "${TEXT2SQL_DATABASE_URL}",
        "TEXT2SQL_TRACE_MODE": "local",
        "TEXT2SQL_WORKSPACE_DIR": ".text2sql"
      }
    }
  }
}
```

## Databricks SQL setup

Install/scaffold the Databricks driver extra and keep local tracing enabled:

```bash
text2sql init --database-type databricks --trace-mode local
export TEXT2SQL_DATABASE_URL='databricks://token:<PAT>@<workspace-host>?http_path=<warehouse-http-path>&catalog=<catalog>&schema=<schema>'
claude
```

Obtain the workspace host and HTTP path from the SQL warehouse connection
details. Use a dedicated principal/token with only warehouse use, catalog/schema
use, and `SELECT` privileges. URL-encode connection values when they contain URL
special characters. Do not commit the URL or token.

Keep state and traces local (the default), or send traces to a separate Postgres
control plane:

```bash
text2sql init --database-type databricks --trace-mode database --trace-database-type postgres
export TEXT2SQL_TRACE_DATABASE_URL='postgresql://text2sql_writer:<password>@<host>/agent_observability'
```

This installs both drivers and adds the trace URL environment reference without
storing either credential. Databricks source-database tracing is rejected by the
coding-assistant runtime. The dialect cannot establish a read-only transaction,
so dedicated Databricks `SELECT`-only privileges—not the framework's lexical SQL
filter—are the authoritative write boundary.

## Host-agent tools

- `start_query(question)` — starts a traced investigation and returns a `query_id`.
- `run_python(query_id, code)` — persistent restricted Python with `db`, `traces`,
  `skills`, and schema capabilities.
- `finish_query(query_id, sql, max_rows=100)` — requires the exact final SQL to have been
  successfully tested with `db.query()` and persists the completed trace.
- `abort_query(query_id, error)` — persists an abandoned investigation as a failed trace.
- `recent_traces(limit=10)` — reads completed traces.

The legacy `query(question)` tool is retained for autonomous model-backed use,
but unlike the host-agent tools it requires a provider extra and API key.

## Trace storage

Local JSONL is the default:

```text
.text2sql/traces.jsonl
```

Write traces into framework-owned tables in the queried database:

```json
"TEXT2SQL_TRACE_MODE": "database"
```

Or use a separate trace database:

```json
"TEXT2SQL_TRACE_MODE": "database",
"TEXT2SQL_TRACE_DATABASE_URL": "postgresql://.../agent_observability"
```

Database tracing requires permission to create and insert into
`text2sql_traces` and `text2sql_tool_calls`. Prefer read-only database credentials
plus local or separate trace storage in production.

## Environment

| Variable | Default | Description |
| --- | --- | --- |
| `TEXT2SQL_DATABASE_URL` | required | SQLAlchemy datasource URL |
| `TEXT2SQL_TRACE_MODE` | `local` | `local`, `database`, or `off` |
| `TEXT2SQL_TRACE_FILE` | `.text2sql/traces.jsonl` | Local JSONL path |
| `TEXT2SQL_TRACE_DATABASE_URL` | source DB | Optional separate DB trace sink |
| `TEXT2SQL_WORKSPACE_DIR` | `.text2sql` | Local skills/state directory |
| `TEXT2SQL_INSTRUCTIONS` | empty | Optional business guidance returned at query start |
| `TEXT2SQL_EXAMPLES` | empty | Optional scenarios file |

## Security

SQL is lexically checked and executed using database-level read-only guards where
supported, but production deployments should still use credentials with only the
minimum read grants. Restricted Python is currently in-process and is not a
hardened sandbox for untrusted users.

## License

MIT
