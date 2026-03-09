from __future__ import annotations

import os

from athenaforge.application.dtos.foundation_dtos import ScaffoldResult
from athenaforge.domain.events.foundation_events import ScaffoldGenerated
from athenaforge.domain.ports.config_port import ConfigPort
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.ports.terraform_port import TerraformGeneratorPort

_TEMPLATES = ("folder.tf", "project.tf", "iam.tf", "bigquery_dataset.tf")


class GenerateScaffoldUseCase:
    """Generate Terraform scaffold files for each LOB in a manifest."""

    def __init__(
        self,
        config_port: ConfigPort,
        terraform_generator: TerraformGeneratorPort,
        event_bus: EventBusPort,
    ) -> None:
        self._config_port = config_port
        self._terraform_generator = terraform_generator
        self._event_bus = event_bus

    async def execute(
        self, manifest_path: str, output_dir: str
    ) -> ScaffoldResult:
        manifest = self._config_port.load_manifest(manifest_path)
        lobs: list[dict] = manifest.get("lobs", [])

        all_files: list[str] = []

        for lob in lobs:
            lob_name: str = lob["name"]
            lob_dir = os.path.join(output_dir, lob_name)
            context = {"lob": lob, "manifest": manifest}

            for template_name in _TEMPLATES:
                content = self._terraform_generator.render_template(
                    template_name, context
                )
                file_path = os.path.join(lob_dir, template_name)
                self._terraform_generator.write_file(file_path, content)
                all_files.append(file_path)

            await self._event_bus.publish(
                ScaffoldGenerated(
                    aggregate_id=lob_name,
                    lob_name=lob_name,
                    terraform_files=tuple(
                        os.path.join(lob_dir, t) for t in _TEMPLATES
                    ),
                )
            )

        last_lob_name = lobs[-1]["name"] if lobs else ""
        return ScaffoldResult(
            lob_name=last_lob_name,
            terraform_files=all_files,
            output_dir=output_dir,
        )
