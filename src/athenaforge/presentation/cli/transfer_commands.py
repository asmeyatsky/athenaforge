from __future__ import annotations

import asyncio
import json

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()


@click.group()
def transfer():
    """M3: TransferForge — Data compaction, egress costing, STS jobs, and DVT"""
    pass


@transfer.command()
@click.option("--bucket", required=True, help="S3 bucket containing Delta tables")
@click.option("--prefixes", required=True, help="Comma-separated table prefixes")
@click.pass_context
def compact(ctx, bucket, prefixes):
    """Plan Delta log compaction for migration readiness"""
    console = Console()
    try:
        container = ctx.obj["container"]
        prefix_list = [p.strip() for p in prefixes.split(",")]
        plans = asyncio.run(container.plan_delta_compaction_use_case.execute(bucket, prefix_list))
        table = Table(title="Compaction Plans")
        table.add_column("Table")
        table.add_column("Current Size", justify="right")
        table.add_column("Est. Reduction %", justify="right")
        table.add_column("Action")
        for plan in plans:
            size_mb = plan.current_size_bytes / 1_048_576
            table.add_row(
                plan.table_name,
                f"{size_mb:.1f} MB",
                f"{plan.estimated_reduction_pct:.0f}%",
                plan.recommended_action,
            )
        console.print(table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@transfer.command(name="egress-cost")
@click.option("--total-size-gb", type=float, required=True, help="Total data size in GB")
@click.option("--credit-pct", type=float, default=0.0, help="AWS credit percentage (0-100)")
@click.pass_context
def egress_cost(ctx, total_size_gb, credit_pct):
    """Model data-egress costs for S3-to-GCS transfer"""
    console = Console()
    try:
        container = ctx.obj["container"]
        total_size_bytes = int(total_size_gb * 1_073_741_824)
        result = asyncio.run(container.model_egress_cost_use_case.execute(total_size_bytes, credit_pct))
        table = Table(title="Egress Cost Estimate")
        table.add_column("Scenario", style="bold")
        table.add_column("Cost (USD)", justify="right")
        table.add_row("Base (no credits)", f"${result.scenario_base_usd:,.2f}")
        table.add_row(f"With {result.credit_percentage:.0f}% credits", f"${result.scenario_with_credits_usd:,.2f}")
        table.add_row("Optimized (compressed)", f"${result.scenario_optimized_usd:,.2f}")
        console.print(table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@transfer.command(name="create-sts")
@click.option("--source-buckets", required=True, help="Comma-separated source S3 bucket names")
@click.option("--dest-bucket", required=True, help="Destination GCS bucket name")
@click.pass_context
def create_sts(ctx, source_buckets, dest_bucket):
    """Create Storage Transfer Service jobs"""
    console = Console()
    try:
        container = ctx.obj["container"]
        bucket_list = [b.strip() for b in source_buckets.split(",")]
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task(description="Creating STS jobs...", total=None)
            results = asyncio.run(container.create_sts_jobs_use_case.execute(bucket_list, dest_bucket))
        table = Table(title="STS Jobs Created")
        table.add_column("Job ID")
        table.add_column("Source Bucket")
        table.add_column("Dest Bucket")
        table.add_column("Status")
        for r in results:
            style = "green" if r.status == "active" else "yellow"
            table.add_row(r.job_id, r.source_bucket, r.dest_bucket, f"[{style}]{r.status}[/{style}]")
        console.print(table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@transfer.command()
@click.option("--tier", required=True, type=click.Choice(["tier1", "tier2", "tier3"]), help="Validation tier level")
@click.option("--pairs-file", required=True, help="JSON file with table pairs [[source, target], ...]")
@click.option("--keys-file", default=None, help="JSON file with primary keys {source: [keys]}")
@click.pass_context
def dvt(ctx, tier, pairs_file, keys_file):
    """Run Data Validation Tool checks"""
    console = Console()
    try:
        container = ctx.obj["container"]
        with open(pairs_file) as f:
            raw_pairs = json.load(f)
        table_pairs = [(p[0], p[1]) for p in raw_pairs]
        primary_keys = None
        if keys_file:
            with open(keys_file) as f:
                primary_keys = json.load(f)
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task(description="Running DVT validation...", total=None)
            result = asyncio.run(container.run_dvt_validation_use_case.execute(tier, table_pairs, primary_keys))
        table = Table(title=f"DVT Validation \u2014 {result.tier}")
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")
        table.add_row("Tables Validated", str(result.tables_validated))
        table.add_row("Passed", f"[green]{result.tables_passed}[/green]")
        table.add_row("Failed", f"[red]{result.tables_failed}[/red]")
        console.print(table)
        if result.details:
            detail_table = Table(title="Details")
            detail_table.add_column("Source")
            detail_table.add_column("Target")
            detail_table.add_column("Status")
            detail_table.add_column("Passed Checks")
            detail_table.add_column("Failed Checks")
            for d in result.details:
                style = "green" if d["status"] == "passed" else "red"
                detail_table.add_row(
                    d["source_table"],
                    d["target_table"],
                    f"[{style}]{d['status']}[/{style}]",
                    d.get("checks_passed", ""),
                    d.get("checks_failed", ""),
                )
            console.print(detail_table)
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)


@transfer.command()
@click.option("--job-id", required=True, help="Streaming job ID")
@click.option("--source-topic", required=True, help="Source Kafka topic")
@click.option("--target-topic", required=True, help="Target Pub/Sub topic")
@click.option("--current-lag", type=int, default=0, help="Current consumer lag")
@click.pass_context
def cutover(ctx, job_id, source_topic, target_topic, current_lag):
    """Control streaming pipeline cutover"""
    console = Console()
    try:
        container = ctx.obj["container"]
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task(description="Executing streaming cutover...", total=None)
            result = asyncio.run(
                container.control_streaming_cutover_use_case.execute(
                    job_id, source_topic, target_topic, current_lag
                )
            )
        console.print(f"[green]\u2713[/green] Cutover for job {result.job_id}: {result.status}")
        console.print(f"  Source: {result.source_topic} \u2192 Target: {result.target_topic}")
        console.print(f"  Lag at cutover: {result.lag_at_cutover}")
    except Exception as exc:
        console.print(f"\n[bold red]Error:[/bold red] {type(exc).__name__}: {exc}")
        raise SystemExit(1)
