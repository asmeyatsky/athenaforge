from __future__ import annotations

import asyncio
import json

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()


@click.group()
def sql():
    """M2: SQLForge — SQL translation, UDF classification, and validation"""
    pass


@sql.command()
@click.option("--source-dir", required=True, help="Directory containing source SQL files")
@click.option("--output-dir", default="./output/translated", help="Output directory for translated SQL")
@click.pass_context
def translate(ctx, source_dir, output_dir):
    """Translate Athena/Presto SQL to BigQuery SQL"""
    console = Console()
    try:
        container = ctx.obj["container"]
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task(description="Translating SQL batch...", total=None)
            result = asyncio.run(container.translate_batch_use_case.execute(source_dir, output_dir))
        console.print(f"[green]\u2713[/green] Batch {result.batch_id} complete")
        table = Table(title="Translation Results")
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")
        table.add_row("Total Files", str(result.total_files))
        table.add_row("Succeeded", f"[green]{result.succeeded}[/green]")
        table.add_row("Failed", f"[red]{result.failed}[/red]")
        table.add_row("Patterns Applied", str(len(result.patterns_applied)))
        console.print(table)
        if result.patterns_applied:
            console.print("\nPatterns applied:")
            for p in result.patterns_applied:
                console.print(f"  - {p}")
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@sql.command(name="map-cascade")
@click.option("--deps-file", required=True, help="JSON file with dependency map {table: [deps]}")
@click.pass_context
def map_cascade(ctx, deps_file):
    """Analyse MAP/CASCADE dependencies between tables"""
    console = Console()
    try:
        container = ctx.obj["container"]
        with open(deps_file) as f:
            dependencies = json.load(f)
        result = asyncio.run(container.analyse_map_cascade_use_case.execute(dependencies))
        console.print(f"[green]\u2713[/green] Analysed {result.total_maps} maps, max cascade depth: {result.cascade_depth}")
        if result.co_migration_batches:
            table = Table(title="Co-Migration Batches")
            table.add_column("Batch #", justify="right")
            table.add_column("Tables")
            for i, batch in enumerate(result.co_migration_batches, 1):
                table.add_row(str(i), ", ".join(batch))
            console.print(table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@sql.command(name="normalise-case")
@click.option("--sql-file", required=True, help="Path to SQL file to normalise")
@click.option("--columns", required=True, help="Comma-separated column names to wrap in UPPER()")
@click.option("--output-file", default=None, help="Output file path (defaults to stdout)")
@click.pass_context
def normalise_case(ctx, sql_file, columns, output_file):
    """Normalise case sensitivity by wrapping columns in UPPER()"""
    console = Console()
    try:
        container = ctx.obj["container"]
        with open(sql_file) as f:
            sql_content = f.read()
        column_list = [c.strip() for c in columns.split(",")]
        result = asyncio.run(container.normalise_case_sensitivity_use_case.execute(sql_content, column_list))
        if output_file:
            with open(output_file, "w") as f:
                f.write(result)
            console.print(f"[green]\u2713[/green] Normalised SQL written to {output_file}")
        else:
            console.print(result)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@sql.command(name="classify-udfs")
@click.option("--udfs-file", required=True, help="JSON file mapping UDF names to bodies")
@click.pass_context
def classify_udfs(ctx, udfs_file):
    """Classify UDFs into SQL, JavaScript, or Cloud Run categories"""
    console = Console()
    try:
        container = ctx.obj["container"]
        with open(udfs_file) as f:
            udfs = json.load(f)
        result = asyncio.run(container.classify_udfs_use_case.execute(udfs))
        table = Table(title="UDF Classification")
        table.add_column("Category", style="bold")
        table.add_column("Count", justify="right")
        table.add_row("SQL UDFs", str(result.sql_udfs))
        table.add_row("JavaScript UDFs", str(result.js_udfs))
        table.add_row("Cloud Run (Remote)", str(result.cloud_run_udfs))
        table.add_row("Total", str(result.total_udfs))
        console.print(table)
        if result.classifications:
            detail_table = Table(title="UDF Details")
            detail_table.add_column("UDF Name")
            detail_table.add_column("Category")
            for name, category in result.classifications.items():
                detail_table.add_row(name, category)
            console.print(detail_table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@sql.command()
@click.option("--query-dir", required=True, help="Directory containing translated SQL queries")
@click.pass_context
def validate(ctx, query_dir):
    """Validate translated queries via BigQuery dry-run"""
    console = Console()
    try:
        import os

        container = ctx.obj["container"]
        query_paths = []
        query_contents = {}
        for fname in sorted(os.listdir(query_dir)):
            if fname.endswith(".sql"):
                fpath = os.path.join(query_dir, fname)
                query_paths.append(fpath)
                with open(fpath) as f:
                    query_contents[fpath] = f.read()
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task(description="Validating translated queries...", total=None)
            result = asyncio.run(container.validate_queries_use_case.execute(query_paths, query_contents))
        table = Table(title="Validation Results")
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")
        table.add_row("Total Queries", str(result.total_queries))
        table.add_row("Passed", f"[green]{result.passed}[/green]")
        table.add_row("Failed", f"[red]{result.failed}[/red]")
        table.add_row("Bytes Scanned", f"{result.total_bytes_scanned:,}")
        console.print(table)
        if result.failures:
            fail_table = Table(title="Failures")
            fail_table.add_column("Query Path")
            fail_table.add_column("Error")
            for f in result.failures:
                fail_table.add_row(f["query_path"], f["error"])
            console.print(fail_table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)
