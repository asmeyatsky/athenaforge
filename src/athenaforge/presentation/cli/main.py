from __future__ import annotations

import click

from athenaforge.infrastructure.config import AppConfig, DependencyContainer


@click.group()
@click.option("--config", "config_path", default=None, help="Path to config YAML file")
@click.pass_context
def cli(ctx: click.Context, config_path: str | None) -> None:
    """AthenaForge™ — Athena-to-BigQuery Migration Accelerator"""
    ctx.ensure_object(dict)
    if config_path:
        config = AppConfig.from_yaml(config_path)
    else:
        config = AppConfig.from_env()
    ctx.obj["config"] = config
    ctx.obj["container"] = DependencyContainer(config)


# Import and register command groups
from athenaforge.presentation.cli.foundation_commands import foundation
from athenaforge.presentation.cli.sql_commands import sql
from athenaforge.presentation.cli.transfer_commands import transfer
from athenaforge.presentation.cli.wave_commands import wave
from athenaforge.presentation.cli.dependency_commands import dependency
from athenaforge.presentation.cli.migrate_commands import migrate

cli.add_command(foundation)
cli.add_command(sql)
cli.add_command(transfer)
cli.add_command(wave)
cli.add_command(dependency)
cli.add_command(migrate)

if __name__ == "__main__":
    cli()
