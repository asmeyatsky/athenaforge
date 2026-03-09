"""Application-layer tests for GenerateScaffoldUseCase."""

from __future__ import annotations

import pytest

from athenaforge.application.commands.foundation.generate_scaffold import (
    GenerateScaffoldUseCase,
)
from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.foundation_events import ScaffoldGenerated


# ── Stub ports ───────────────────────────────────────────────────────────────


class StubConfigPort:
    """Returns a pre-configured manifest dict."""

    def __init__(self, manifest: dict) -> None:
        self._manifest = manifest

    def load_manifest(self, path: str) -> dict:
        return self._manifest

    def save_manifest(self, path: str, data: dict) -> None:
        pass  # not used in this use case


class StubTerraformGeneratorPort:
    """Records every template render and file write."""

    def __init__(self) -> None:
        self.rendered: list[tuple[str, dict]] = []
        self.written_files: list[tuple[str, str]] = []

    def render_template(self, template_name: str, context: dict) -> str:
        self.rendered.append((template_name, context))
        return f"# rendered {template_name}"

    def write_file(self, output_path: str, content: str) -> None:
        self.written_files.append((output_path, content))


class StubEventBus:
    """Collects all published events."""

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        self.events.append(event)

    def subscribe(self, event_type: type, handler: object) -> None:
        pass


# ── Fixtures ─────────────────────────────────────────────────────────────────

SAMPLE_MANIFEST = {
    "project": "athenaforge-demo",
    "lobs": [
        {"name": "finance", "datasets": ["transactions"]},
        {"name": "marketing", "datasets": ["campaigns", "clicks"]},
    ],
}

EXPECTED_TEMPLATES = ("folder.tf", "project.tf", "iam.tf", "bigquery_dataset.tf")


# ── Tests ────────────────────────────────────────────────────────────────────


async def test_execute_generates_terraform_files_for_each_lob():
    """Each LOB should produce one file per template."""
    config = StubConfigPort(SAMPLE_MANIFEST)
    tf = StubTerraformGeneratorPort()
    bus = StubEventBus()

    uc = GenerateScaffoldUseCase(config, tf, bus)
    result = await uc.execute("/fake/manifest.yaml", "/output")

    # 2 LOBs * 4 templates = 8 files
    assert len(tf.written_files) == 8

    # Verify file paths contain the LOB name and template name
    written_paths = [path for path, _ in tf.written_files]
    for lob in ("finance", "marketing"):
        for tmpl in EXPECTED_TEMPLATES:
            expected = f"/output/{lob}/{tmpl}"
            assert expected in written_paths, f"Missing {expected}"


async def test_execute_renders_templates_with_correct_context():
    """render_template must receive the LOB dict and full manifest."""
    config = StubConfigPort(SAMPLE_MANIFEST)
    tf = StubTerraformGeneratorPort()
    bus = StubEventBus()

    uc = GenerateScaffoldUseCase(config, tf, bus)
    await uc.execute("/fake/manifest.yaml", "/output")

    # 8 renders total
    assert len(tf.rendered) == 8

    # First 4 renders are for 'finance'
    for template_name, context in tf.rendered[:4]:
        assert context["lob"]["name"] == "finance"
        assert context["manifest"] is SAMPLE_MANIFEST

    # Next 4 are for 'marketing'
    for template_name, context in tf.rendered[4:]:
        assert context["lob"]["name"] == "marketing"


async def test_scaffold_generated_event_published_per_lob():
    """A ScaffoldGenerated event must be published for every LOB."""
    config = StubConfigPort(SAMPLE_MANIFEST)
    tf = StubTerraformGeneratorPort()
    bus = StubEventBus()

    uc = GenerateScaffoldUseCase(config, tf, bus)
    await uc.execute("/fake/manifest.yaml", "/output")

    scaffold_events = [e for e in bus.events if isinstance(e, ScaffoldGenerated)]
    assert len(scaffold_events) == 2

    lob_names = [e.lob_name for e in scaffold_events]
    assert lob_names == ["finance", "marketing"]

    # Each event should list exactly the 4 terraform files
    for event in scaffold_events:
        assert len(event.terraform_files) == 4


async def test_result_contains_all_generated_files():
    """The returned ScaffoldResult should list every generated file."""
    config = StubConfigPort(SAMPLE_MANIFEST)
    tf = StubTerraformGeneratorPort()
    bus = StubEventBus()

    uc = GenerateScaffoldUseCase(config, tf, bus)
    result = await uc.execute("/fake/manifest.yaml", "/output")

    assert result.output_dir == "/output"
    assert len(result.terraform_files) == 8
    assert result.lob_name == "marketing"  # last LOB processed


async def test_empty_manifest_produces_no_files():
    """An empty LOB list should generate zero files and zero events."""
    config = StubConfigPort({"lobs": []})
    tf = StubTerraformGeneratorPort()
    bus = StubEventBus()

    uc = GenerateScaffoldUseCase(config, tf, bus)
    result = await uc.execute("/fake/manifest.yaml", "/output")

    assert len(tf.written_files) == 0
    assert len(bus.events) == 0
    assert result.terraform_files == []
    assert result.lob_name == ""


async def test_empty_manifest_no_lobs_key():
    """Manifest without the 'lobs' key should behave like an empty list."""
    config = StubConfigPort({"project": "demo"})
    tf = StubTerraformGeneratorPort()
    bus = StubEventBus()

    uc = GenerateScaffoldUseCase(config, tf, bus)
    result = await uc.execute("/fake/manifest.yaml", "/output")

    assert len(tf.written_files) == 0
    assert len(bus.events) == 0
    assert result.terraform_files == []


async def test_single_lob_generates_correct_count():
    """A manifest with one LOB should produce exactly 4 files."""
    manifest = {"lobs": [{"name": "data_eng"}]}
    config = StubConfigPort(manifest)
    tf = StubTerraformGeneratorPort()
    bus = StubEventBus()

    uc = GenerateScaffoldUseCase(config, tf, bus)
    result = await uc.execute("/fake/manifest.yaml", "/out")

    assert len(tf.written_files) == 4
    assert len(bus.events) == 1
    assert result.lob_name == "data_eng"
