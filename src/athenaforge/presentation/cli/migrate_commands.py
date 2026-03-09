from __future__ import annotations

import asyncio

import click
from rich.console import Console

console = Console()


@click.group()
def migrate():
    """End-to-end migration orchestration"""
    pass


@migrate.command()
@click.option("--manifest", required=True, help="Path to LOB manifest YAML")
@click.option("--output-dir", default="./output/terraform", help="Terraform output directory")
@click.option("--inventory-id", required=True, help="Table inventory ID")
@click.option("--source-dir", required=True, help="Directory containing source SQL files")
@click.option("--translated-dir", default="./output/translated", help="Output directory for translated SQL")
@click.option("--bucket", required=True, help="S3 bucket for dependency scanning")
@click.option("--prefixes", default="", help="Comma-separated prefixes for dependency scanning")
@click.option("--lobs", required=True, help="Comma-separated LOB names")
@click.option("--max-parallel", type=int, default=3, help="Max parallel waves")
@click.pass_context
def full(ctx, manifest, output_dir, inventory_id, source_dir, translated_dir, bucket, prefixes, lobs, max_parallel):
    """Run the full end-to-end MigrationWorkflow"""
    from athenaforge.application.orchestration.migration_workflow import MigrationWorkflow

    container = ctx.obj["container"]

    workflow = MigrationWorkflow(
        scaffold_uc=container.generate_scaffold_use_case,
        classify_uc=container.classify_tiers_use_case,
        translate_uc=container.translate_batch_use_case,
        scan_deps_uc=container.scan_spark_flink_jobs_use_case,
        validate_uc=container.validate_queries_use_case,
        rewrite_dags_uc=container.rewrite_dags_use_case,
        plan_waves_uc=container.plan_waves_use_case,
    )

    orchestrator = workflow.build()
    console.print("[bold]Starting full migration workflow...[/bold]\n")

    step_results = asyncio.run(orchestrator.execute())

    for step_result in step_results:
        if step_result.success:
            console.print(
                f"[green]✓[/green] {step_result.step_name}: complete"
                f" ({step_result.duration_seconds:.1f}s)"
            )
        else:
            console.print(
                f"[red]✗[/red] {step_result.step_name}: {step_result.error}"
            )

    succeeded = sum(1 for r in step_results if r.success)
    failed = len(step_results) - succeeded
    console.print(f"\n[bold]Results: {succeeded} succeeded, {failed} failed[/bold]")
    if failed == 0:
        console.print("[bold green]Migration workflow complete.[/bold green]")
    else:
        console.print("[bold red]Migration workflow finished with errors.[/bold red]")
