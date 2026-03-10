from __future__ import annotations

import asyncio
import json

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()


@click.group()
def dependency():
    """M5: DependencyForge — Spark/Flink scanning, DAG rewriting, Kafka, Lambda, IAM"""
    pass


@dependency.command()
@click.option("--bucket", required=True, help="S3 bucket to scan for job files")
@click.option("--prefixes", required=True, help="Comma-separated path prefixes to scan")
@click.pass_context
def scan(ctx, bucket, prefixes):
    """Scan for Spark/Flink jobs and identify Athena dependencies"""
    console = Console()
    try:
        container = ctx.obj["container"]
        prefix_list = [p.strip() for p in prefixes.split(",")]
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task(description="Scanning for Spark/Flink jobs...", total=None)
            result = asyncio.run(container.scan_spark_flink_jobs_use_case.execute(bucket, prefix_list))
        table = Table(title="Dependency Scan Results")
        table.add_column("Job Type", style="bold")
        table.add_column("Count", justify="right")
        table.add_row("Spark Jobs", str(result.spark_jobs))
        table.add_row("Flink Jobs", str(result.flink_jobs))
        table.add_row("Airflow DAGs", str(result.dags))
        table.add_row("Lambda Functions", str(result.lambdas))
        table.add_row("Total References", str(result.total_references))
        console.print(table)
        if result.details:
            detail_table = Table(title="Scan Details")
            detail_table.add_column("File")
            detail_table.add_column("Type")
            detail_table.add_column("References")
            for d in result.details:
                refs = ", ".join(str(r) for r in d.get("references", []))
                detail_table.add_row(str(d["file"]), str(d["job_type"]), refs)
            console.print(detail_table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@dependency.command(name="rewrite-dags")
@click.option("--dags-dir", required=True, help="Directory containing Airflow DAG files")
@click.pass_context
def rewrite_dags(ctx, dags_dir):
    """Rewrite Airflow DAGs from AWS to GCP operators"""
    console = Console()
    try:
        import os

        container = ctx.obj["container"]
        dag_contents: dict[str, str] = {}
        for fname in sorted(os.listdir(dags_dir)):
            if fname.endswith(".py"):
                fpath = os.path.join(dags_dir, fname)
                with open(fpath) as f:
                    dag_contents[fpath] = f.read()
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task(description="Rewriting Airflow DAGs...", total=None)
            result = asyncio.run(container.rewrite_dags_use_case.execute(dag_contents))
        console.print(f"[green]\u2713[/green] Processed {result.dags_processed} DAGs, rewrote {result.dags_rewritten}")
        table = Table(title="DAG Rewrite Results")
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")
        table.add_row("DAGs Processed", str(result.dags_processed))
        table.add_row("DAGs Rewritten", str(result.dags_rewritten))
        table.add_row("Operators Replaced", str(result.operators_replaced))
        console.print(table)
        if result.changes:
            change_table = Table(title="Changes")
            change_table.add_column("File")
            change_table.add_column("Change")
            for c in result.changes:
                change_table.add_row(c["file"], c["change"])
            console.print(change_table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@dependency.command()
@click.option("--topics-file", required=True, help="JSON file with Kafka topic configs [{topic: ..., schema: ...}]")
@click.pass_context
def kafka(ctx, topics_file):
    """Migrate Kafka topic configurations"""
    console = Console()
    try:
        container = ctx.obj["container"]
        with open(topics_file) as f:
            topic_configs = json.load(f)
        result = asyncio.run(container.migrate_kafka_topics_use_case.execute(topic_configs))
        table = Table(title="Kafka Topic Migration")
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")
        table.add_row("Topics Migrated", str(result.topics_migrated))
        table.add_row("Schemas Updated", str(result.schemas_updated))
        console.print(table)
        if result.details:
            detail_table = Table(title="Details")
            detail_table.add_column("Topic")
            detail_table.add_column("Status")
            for d in result.details:
                detail_table.add_row(d["topic"], d["status"])
            console.print(detail_table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@dependency.command()
@click.option("--sources-file", required=True, help="JSON file mapping function names to source code")
@click.pass_context
def lambdas(ctx, sources_file):
    """Scan and rewrite Lambda functions with Athena references"""
    console = Console()
    try:
        container = ctx.obj["container"]
        with open(sources_file) as f:
            lambda_sources = json.load(f)
        result = asyncio.run(container.rewrite_lambdas_use_case.execute(lambda_sources))
        table = Table(title="Lambda Rewrite Results")
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")
        table.add_row("Functions Processed", str(result.functions_processed))
        table.add_row("Functions Rewritten", str(result.functions_rewritten))
        console.print(table)
        if result.details:
            detail_table = Table(title="Details")
            detail_table.add_column("Function")
            detail_table.add_column("Status")
            for d in result.details:
                style = "yellow" if d["status"] == "needs_rewrite" else "green"
                detail_table.add_row(d["function"], f"[{style}]{d['status']}[/{style}]")
            console.print(detail_table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@dependency.command()
@click.option("--policies-file", required=True, help="JSON file with Lake Formation policies [{permission, resource, principal}]")
@click.pass_context
def iam(ctx, policies_file):
    """Map Lake Formation permissions to BigQuery IAM roles"""
    console = Console()
    try:
        container = ctx.obj["container"]
        with open(policies_file) as f:
            policies = json.load(f)
        result = asyncio.run(container.map_iam_permissions_use_case.execute(policies))
        console.print(f"[green]\u2713[/green] Mapped {result.policies_mapped} policies")
        if result.mappings:
            table = Table(title="IAM Permission Mappings")
            table.add_column("Lake Formation Permission")
            table.add_column("Resource")
            table.add_column("Principal")
            table.add_column("BigQuery Role")
            for m in result.mappings:
                table.add_row(
                    m["lake_formation_permission"],
                    m["resource"],
                    m["principal"],
                    m["bigquery_role"],
                )
            console.print(table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)
