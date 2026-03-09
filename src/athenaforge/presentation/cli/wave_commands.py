from __future__ import annotations

import asyncio
import json

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
def wave():
    """M4: WaveForge — Wave planning, parallel runs, gates, and KPI reconciliation"""
    pass


@wave.command()
@click.option("--inventory-id", required=True, help="Table inventory ID")
@click.option("--lobs", required=True, help="Comma-separated LOB names")
@click.option("--max-parallel", type=int, default=3, help="Max parallel waves")
@click.pass_context
def plan(ctx, inventory_id, lobs, max_parallel):
    """Plan migration waves based on inventory and LOBs"""
    container = ctx.obj["container"]
    lob_list = [l.strip() for l in lobs.split(",")]
    result = asyncio.run(container.plan_waves_use_case.execute(inventory_id, lob_list, max_parallel))
    console.print(f"[green]✓[/green] Planned {result.total_waves} waves for {result.total_tables} tables")
    table = Table(title="Wave Plan")
    table.add_column("Wave ID")
    table.add_column("Name")
    table.add_column("LOB")
    table.add_column("Tables", justify="right")
    table.add_column("Est. Days", justify="right")
    for w in result.waves:
        table.add_row(
            str(w["wave_id"]),
            str(w["name"]),
            str(w["lob"]),
            str(w["table_count"]),
            str(w["estimated_days"]),
        )
    console.print(table)


@wave.command(name="parallel-run")
@click.option("--wave-id", required=True, help="Wave ID")
@click.option("--target-mode", required=True, help="Target parallel-run mode")
@click.pass_context
def parallel_run(ctx, wave_id, target_mode):
    """Control parallel-run mode for a wave"""
    container = ctx.obj["container"]
    result = asyncio.run(container.control_parallel_run_use_case.execute(wave_id, target_mode))
    if result.success:
        console.print(f"[green]✓[/green] Wave {result.wave_id}: {result.previous_mode} → {result.current_mode}")
    else:
        console.print(f"[red]✗[/red] Failed to transition wave {result.wave_id}")


@wave.command(name="rollback-check")
@click.option("--wave-id", required=True, help="Wave ID to evaluate")
@click.option("--dvt-pass-rate", type=float, required=True, help="DVT pass rate (0.0 to 1.0)")
@click.option("--latency-increase-pct", type=float, default=0.0, help="Latency increase percentage")
@click.option("--data-loss-detected", is_flag=True, help="Flag if data loss detected")
@click.option("--streaming-lag", type=int, default=0, help="Current streaming lag")
@click.option("--escalation-raised", is_flag=True, help="Flag if escalation raised")
@click.pass_context
def rollback_check(ctx, wave_id, dvt_pass_rate, latency_increase_pct, data_loss_detected, streaming_lag, escalation_raised):
    """Evaluate whether a wave should be rolled back"""
    container = ctx.obj["container"]
    result = asyncio.run(
        container.evaluate_rollback_use_case.execute(
            wave_id, dvt_pass_rate, latency_increase_pct,
            data_loss_detected, streaming_lag, escalation_raised,
        )
    )
    if result.should_rollback:
        console.print(f"[bold red]ROLLBACK RECOMMENDED[/bold red] for wave {wave_id}")
    else:
        console.print(f"[green]✓[/green] No rollback needed for wave {wave_id}")
    table = Table(title="Rollback Conditions")
    table.add_column("Condition", style="bold")
    table.add_column("Triggered")
    table.add_column("Details")
    for c in result.conditions:
        style = "red" if c["triggered"] else "green"
        triggered_str = "Yes" if c["triggered"] else "No"
        table.add_row(str(c["name"]), f"[{style}]{triggered_str}[/{style}]", str(c["details"]))
    console.print(table)


@wave.command()
@click.option("--wave-id", required=True, help="Wave ID to gate-check")
@click.option("--criteria-file", required=True, help="JSON file with criteria {name: bool}")
@click.pass_context
def gate(ctx, wave_id, criteria_file):
    """Enforce quality gate for a wave"""
    container = ctx.obj["container"]
    with open(criteria_file) as f:
        criteria = json.load(f)
    result = asyncio.run(container.enforce_wave_gate_use_case.execute(wave_id, criteria))
    if result.passed:
        console.print(f"[green]✓ GATE PASSED[/green] for wave {result.wave_id}")
    else:
        console.print(f"[red]✗ GATE FAILED[/red] for wave {result.wave_id}")
    if result.criteria_met:
        console.print(f"  Met: {', '.join(result.criteria_met)}")
    if result.criteria_failed:
        console.print(f"  Failed: {', '.join(result.criteria_failed)}")


@wave.command()
@click.option("--configs-file", required=True, help="JSON file with dashboard configs [{name: ...}]")
@click.pass_context
def dashboards(ctx, configs_file):
    """Migrate dashboards to the new platform"""
    container = ctx.obj["container"]
    with open(configs_file) as f:
        configs = json.load(f)
    result = asyncio.run(container.migrate_dashboards_use_case.execute(configs))
    table = Table(title="Dashboard Migration")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_row("Migrated", f"[green]{result.dashboards_migrated}[/green]")
    table.add_row("Failed", f"[red]{result.dashboards_failed}[/red]")
    console.print(table)
    if result.details:
        detail_table = Table(title="Details")
        detail_table.add_column("Dashboard")
        detail_table.add_column("Status")
        for d in result.details:
            style = "green" if d["status"] == "migrated" else "red"
            detail_table.add_row(d["name"], f"[{style}]{d['status']}[/{style}]")
        console.print(detail_table)


@wave.command()
@click.option("--kpis-file", required=True, help="JSON file with KPI definitions [{name: ...}]")
@click.pass_context
def kpi(ctx, kpis_file):
    """Reconcile KPIs between legacy and new platform"""
    container = ctx.obj["container"]
    with open(kpis_file) as f:
        kpi_definitions = json.load(f)
    result = asyncio.run(container.reconcile_kpis_use_case.execute(kpi_definitions))
    table = Table(title="KPI Reconciliation")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_row("Total KPIs", str(result.total_kpis))
    table.add_row("Matched", f"[green]{result.matched}[/green]")
    table.add_row("Mismatched", f"[red]{result.mismatched}[/red]")
    console.print(table)
    if result.details:
        detail_table = Table(title="Details")
        detail_table.add_column("KPI")
        detail_table.add_column("Status")
        for d in result.details:
            style = "green" if d["status"] == "matched" else "red"
            detail_table.add_row(d["name"], f"[{style}]{d['status']}[/{style}]")
        console.print(detail_table)
