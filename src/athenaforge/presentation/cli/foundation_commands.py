from __future__ import annotations

import asyncio

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
def foundation():
    """M1: FoundationForge — Project scaffolding and tier classification"""
    pass


@foundation.command()
@click.option("--manifest", required=True, help="Path to LOB manifest YAML")
@click.option("--output-dir", default="./output/terraform", help="Terraform output directory")
@click.pass_context
def scaffold(ctx, manifest, output_dir):
    """Generate Terraform scaffold for all LOBs"""
    container = ctx.obj["container"]
    result = asyncio.run(container.generate_scaffold_use_case.execute(manifest, output_dir))
    console.print(f"[green]✓[/green] Scaffold generated for {result.lob_name}")
    console.print(f"  Files: {', '.join(result.terraform_files)}")


@foundation.command()
@click.option("--inventory-id", required=True, help="Table inventory ID")
@click.pass_context
def classify(ctx, inventory_id):
    """Classify tables into tiers"""
    container = ctx.obj["container"]
    result = asyncio.run(container.classify_tiers_use_case.execute(inventory_id))
    table = Table(title="Tier Classification")
    table.add_column("Tier", style="bold")
    table.add_column("Count", justify="right")
    table.add_row("Tier 1 (Active, Small)", str(result.tier1_count))
    table.add_row("Tier 2 (Active, Large)", str(result.tier2_count))
    table.add_row("Tier 3 (Inactive)", str(result.tier3_count))
    console.print(table)


@foundation.command()
@click.option("--slots", type=int, required=True, help="Number of BigQuery slots")
@click.option("--commitment-years", type=int, default=3, help="Commitment period (1 or 3 years)")
@click.option("--output-dir", default="./output/terraform", help="Output directory")
@click.pass_context
def pricing(ctx, slots, commitment_years, output_dir):
    """Configure BigQuery slot pricing"""
    container = ctx.obj["container"]
    result = asyncio.run(container.configure_pricing_use_case.execute(slots, commitment_years, output_dir))
    console.print(f"[green]✓[/green] {result.edition} edition: {result.slots} slots @ ${result.monthly_cost_usd:.2f}/month")


@foundation.command(name="delta-health")
@click.option("--bucket", required=True, help="S3/GCS bucket with Delta tables")
@click.option("--prefix", default="", help="Table prefix filter")
@click.pass_context
def delta_health(ctx, bucket, prefix):
    """Check Delta log health for migration readiness"""
    container = ctx.obj["container"]
    results = asyncio.run(container.check_delta_health_use_case.execute(bucket, prefix))
    table = Table(title="Delta Log Health")
    table.add_column("Table")
    table.add_column("Size (MB)", justify="right")
    table.add_column("Status")
    table.add_column("Recommendation")
    for r in results:
        style = {"HEALTHY": "green", "WARNING": "yellow", "CRITICAL": "red", "BLOCKED": "bold red"}.get(r.status, "")
        table.add_row(r.table_name, f"{r.log_size_mb:.1f}", f"[{style}]{r.status}[/{style}]", r.recommendation)
    console.print(table)
