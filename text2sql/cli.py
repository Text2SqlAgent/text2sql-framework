"""CLI interface for text2sql."""

from __future__ import annotations

import sys

import click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table as RichTable

from text2sql.core import TextSQL

console = Console()


@click.group()
@click.version_option(package_name="text2sql")
def main():
    """text2sql - Ask your database questions in plain English."""
    pass


@main.command()
@click.argument("connection_string")
@click.option("--model", default="anthropic:claude-sonnet-4-6", help="Model (e.g. anthropic:claude-sonnet-4-6, openai:gpt-4o, ollama:llama3)")
@click.option("--metadata-hint", default=None, help="Tell the LLM where custom metadata lives")
def ask(connection_string, model, metadata_hint):
    """Interactive question mode."""
    try:
        engine = TextSQL(connection_string, model=model, metadata_hint=metadata_hint)
    except Exception as e:
        console.print(f"[red]Failed to connect: {e}[/red]")
        sys.exit(1)

    console.print(Panel.fit(
        f"Connected to {engine.db.dialect} database",
        title="text2sql", border_style="green",
    ))
    console.print("[dim]Type questions, or 'quit' to exit.[/dim]\n")

    while True:
        try:
            question = console.input("[bold cyan]Q:[/bold cyan] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nBye!")
            break

        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            break

        with console.status("[bold green]Thinking..."):
            result = engine.ask(question)

        if result.sql:
            console.print()
            console.print(Syntax(result.sql, "sql", theme="monokai", line_numbers=False))
            console.print(f"[dim]({result.tool_calls_made} tool calls)[/dim]")

        if result.error:
            console.print(f"\n[red]Error: {result.error}[/red]")
        elif result.data:
            _print_table(result.data)
        console.print()


@main.command()
@click.argument("connection_string")
@click.argument("question")
@click.option("--model", default="anthropic:claude-sonnet-4-6")
@click.option("--metadata-hint", default=None)
@click.option("--json-output", is_flag=True, help="Output JSON")
def query(connection_string, question, model, metadata_hint, json_output):
    """Ask a single question (non-interactive)."""
    import json as json_mod

    engine = TextSQL(connection_string, model=model, metadata_hint=metadata_hint)
    result = engine.ask(question)

    if json_output:
        click.echo(json_mod.dumps({
            "question": result.question,
            "sql": result.sql,
            "data": result.data,
            "error": result.error,
            "tool_calls": result.tool_calls_made,
        }, indent=2, default=str))
    else:
        if result.sql:
            click.echo(result.sql)
        if result.error:
            click.echo(f"Error: {result.error}", err=True)
            sys.exit(1)
        elif result.data:
            _print_table(result.data)


def _print_table(data, max_rows=50):
    """Pretty-print query results."""
    if not data:
        console.print("[dim]No rows[/dim]")
        return

    table = RichTable(show_lines=False, row_styles=["", "dim"])
    for col in data[0].keys():
        table.add_column(str(col))
    for row in data[:max_rows]:
        table.add_row(*[str(v) for v in row.values()])

    if len(data) > max_rows:
        console.print(f"[dim]Showing {max_rows} of {len(data)} rows[/dim]")
    console.print(table)
