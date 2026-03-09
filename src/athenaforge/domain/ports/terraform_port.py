from __future__ import annotations

from typing import Protocol


class TerraformGeneratorPort(Protocol):
    """Port for generating Terraform configuration files."""

    def render_template(self, template_name: str, context: dict) -> str: ...

    def write_file(self, output_path: str, content: str) -> None: ...


class TerraformRunnerPort(Protocol):
    """Port for executing Terraform CLI operations."""

    async def init(self, working_dir: str) -> str: ...

    async def plan(self, working_dir: str) -> str: ...

    async def apply(self, working_dir: str) -> str: ...
