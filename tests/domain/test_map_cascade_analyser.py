"""Tests for MapCascadeAnalyser — pure domain logic, no mocks."""
from __future__ import annotations

import pytest

from athenaforge.domain.services.map_cascade_analyser import (
    CascadeResult,
    MapCascadeAnalyser,
)


class TestAnalyseLinearChain:
    def test_simple_linear_chain(self):
        """A -> B -> C should produce one root (A) with depth 2."""
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B"], "B": ["C"]}
        results = analyser.analyse(deps)

        # A is the root (not depended on by anything)
        root_result = next(r for r in results if r.root_table == "A")
        assert "B" in root_result.dependent_tables
        assert "C" in root_result.dependent_tables
        assert root_result.cascade_depth == 2

    def test_linear_chain_depth(self):
        """A -> B -> C -> D should have depth 3."""
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B"], "B": ["C"], "C": ["D"]}
        results = analyser.analyse(deps)

        root_result = next(r for r in results if r.root_table == "A")
        assert root_result.cascade_depth == 3
        assert len(root_result.dependent_tables) == 3


class TestAnalyseTree:
    def test_tree_structure(self):
        """A -> B and A -> C should produce one root (A) with depth 1."""
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B", "C"]}
        results = analyser.analyse(deps)

        assert len(results) == 1
        root_result = results[0]
        assert root_result.root_table == "A"
        assert set(root_result.dependent_tables) == {"B", "C"}
        assert root_result.cascade_depth == 1

    def test_deeper_tree(self):
        """A -> B, A -> C, B -> D should have depth 2."""
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B", "C"], "B": ["D"]}
        results = analyser.analyse(deps)

        root_result = next(r for r in results if r.root_table == "A")
        assert set(root_result.dependent_tables) == {"B", "C", "D"}
        assert root_result.cascade_depth == 2


class TestAnalyseNoDependencies:
    def test_empty_dependencies(self):
        analyser = MapCascadeAnalyser()
        deps: dict[str, list[str]] = {}
        results = analyser.analyse(deps)

        assert results == []

    def test_single_node_no_children(self):
        """A node with no outgoing edges should still be a root with depth 0."""
        analyser = MapCascadeAnalyser()
        deps = {"A": []}
        results = analyser.analyse(deps)

        assert len(results) == 1
        assert results[0].root_table == "A"
        assert results[0].dependent_tables == ()
        assert results[0].cascade_depth == 0


class TestAnalyseMultipleRoots:
    def test_two_separate_chains(self):
        """A -> B and C -> D should produce two roots."""
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B"], "C": ["D"]}
        results = analyser.analyse(deps)

        root_names = {r.root_table for r in results}
        assert root_names == {"A", "C"}

    def test_roots_sorted_alphabetically(self):
        analyser = MapCascadeAnalyser()
        deps = {"Z": ["Y"], "A": ["B"]}
        results = analyser.analyse(deps)

        root_names = [r.root_table for r in results]
        assert root_names == ["A", "Z"]


class TestGetCoMigrationBatchesConnected:
    def test_linear_chain_single_batch(self):
        """A -> B -> C are all connected and form a single batch."""
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B"], "B": ["C"]}
        batches = analyser.get_co_migration_batches(deps)

        assert len(batches) == 1
        assert set(batches[0]) == {"A", "B", "C"}

    def test_tree_single_batch(self):
        """A -> B, A -> C are all connected."""
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B", "C"]}
        batches = analyser.get_co_migration_batches(deps)

        assert len(batches) == 1
        assert set(batches[0]) == {"A", "B", "C"}


class TestGetCoMigrationBatchesSeparateComponents:
    def test_two_separate_components(self):
        """A -> B and C -> D should produce two separate batches."""
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B"], "C": ["D"]}
        batches = analyser.get_co_migration_batches(deps)

        assert len(batches) == 2
        batch_sets = [set(b) for b in batches]
        assert {"A", "B"} in batch_sets
        assert {"C", "D"} in batch_sets

    def test_three_isolated_nodes(self):
        """Three nodes with no connections form three separate batches."""
        analyser = MapCascadeAnalyser()
        deps = {"A": [], "B": [], "C": []}
        batches = analyser.get_co_migration_batches(deps)

        assert len(batches) == 3

    def test_mixed_connected_and_isolated(self):
        """A -> B connected; C isolated."""
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B"], "C": []}
        batches = analyser.get_co_migration_batches(deps)

        assert len(batches) == 2
        batch_sets = [set(b) for b in batches]
        assert {"A", "B"} in batch_sets
        assert {"C"} in batch_sets


class TestGetCoMigrationBatchesSorted:
    def test_batches_internally_sorted(self):
        analyser = MapCascadeAnalyser()
        deps = {"Z": ["A"], "M": ["B"]}
        batches = analyser.get_co_migration_batches(deps)

        for batch in batches:
            assert list(batch) == sorted(batch)


class TestCascadeResultStructure:
    def test_result_is_frozen(self):
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B"]}
        results = analyser.analyse(deps)

        with pytest.raises(AttributeError):
            results[0].root_table = "X"  # type: ignore[misc]

    def test_dependent_tables_is_tuple(self):
        analyser = MapCascadeAnalyser()
        deps = {"A": ["B", "C"]}
        results = analyser.analyse(deps)

        assert isinstance(results[0].dependent_tables, tuple)
