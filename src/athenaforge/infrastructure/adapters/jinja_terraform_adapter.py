from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from athenaforge.domain.ports.terraform_port import TerraformGeneratorPort


class JinjaTerraformAdapter:
    """Implements TerraformGeneratorPort using Jinja2 templates."""

    def __init__(self, template_dir: str) -> None:
        self._template_dir = template_dir
        self._env = Environment(
            loader=FileSystemLoader(template_dir),
            keep_trailing_newline=True,
        )

    def render_template(self, template_name: str, context: dict) -> str:
        try:
            template = self._env.get_template(template_name)
        except Exception:
            # Fall back to .j2 extension if bare name not found
            template = self._env.get_template(template_name + ".j2")
        return template.render(**context)

    def write_file(self, output_path: str, content: str) -> None:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(content)
