from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from athenaforge.infrastructure.adapters import YamlConfigAdapter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def adapter() -> YamlConfigAdapter:
    return YamlConfigAdapter()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLoadManifest:
    """Tests for YamlConfigAdapter.load_manifest."""

    def test_load_manifest_reads_yaml_file(
        self, adapter: YamlConfigAdapter, tmp_path: Path
    ) -> None:
        data = {"project": "athenaforge", "version": 1, "lobs": ["finance", "hr"]}
        manifest = tmp_path / "manifest.yaml"
        manifest.write_text(yaml.dump(data, default_flow_style=False))

        result = adapter.load_manifest(str(manifest))

        assert isinstance(result, dict)
        assert result["project"] == "athenaforge"
        assert result["version"] == 1
        assert result["lobs"] == ["finance", "hr"]

    def test_load_manifest_empty_file_returns_empty_dict(
        self, adapter: YamlConfigAdapter, tmp_path: Path
    ) -> None:
        manifest = tmp_path / "empty.yaml"
        manifest.write_text("")

        result = adapter.load_manifest(str(manifest))

        assert result == {}

    def test_load_manifest_with_nested_structure(
        self, adapter: YamlConfigAdapter, tmp_path: Path
    ) -> None:
        data = {
            "databases": {
                "source": {"host": "athena.aws", "port": 443},
                "target": {"host": "bigquery.gcp", "port": 443},
            }
        }
        manifest = tmp_path / "nested.yaml"
        manifest.write_text(yaml.dump(data, default_flow_style=False))

        result = adapter.load_manifest(str(manifest))

        assert result["databases"]["source"]["host"] == "athena.aws"
        assert result["databases"]["target"]["host"] == "bigquery.gcp"


class TestSaveManifest:
    """Tests for YamlConfigAdapter.save_manifest."""

    def test_save_manifest_writes_yaml_file(
        self, adapter: YamlConfigAdapter, tmp_path: Path
    ) -> None:
        data = {"project": "athenaforge", "tables": ["orders", "users"]}
        manifest = tmp_path / "output.yaml"

        adapter.save_manifest(str(manifest), data)

        assert manifest.exists()
        loaded = yaml.safe_load(manifest.read_text())
        assert loaded == data

    def test_save_manifest_creates_parent_directories(
        self, adapter: YamlConfigAdapter, tmp_path: Path
    ) -> None:
        data = {"key": "value"}
        deep_path = tmp_path / "a" / "b" / "c" / "manifest.yaml"

        adapter.save_manifest(str(deep_path), data)

        assert deep_path.exists()
        loaded = yaml.safe_load(deep_path.read_text())
        assert loaded == data

    def test_save_then_load_roundtrip(
        self, adapter: YamlConfigAdapter, tmp_path: Path
    ) -> None:
        original = {
            "inventory_id": "inv-001",
            "tables": ["t1", "t2", "t3"],
            "metadata": {"created_by": "test", "count": 3},
        }
        path = tmp_path / "roundtrip.yaml"

        adapter.save_manifest(str(path), original)
        loaded = adapter.load_manifest(str(path))

        assert loaded == original

    def test_save_manifest_overwrites_existing_file(
        self, adapter: YamlConfigAdapter, tmp_path: Path
    ) -> None:
        path = tmp_path / "overwrite.yaml"

        adapter.save_manifest(str(path), {"version": 1})
        adapter.save_manifest(str(path), {"version": 2})

        loaded = adapter.load_manifest(str(path))
        assert loaded == {"version": 2}
