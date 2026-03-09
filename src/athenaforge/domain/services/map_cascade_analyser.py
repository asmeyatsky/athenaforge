from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(frozen=True)
class CascadeResult:
    root_table: str
    dependent_tables: tuple[str, ...]
    cascade_depth: int


class MapCascadeAnalyser:
    """Pure domain service that analyses table dependency cascades."""

    def analyse(
        self, dependencies: dict[str, list[str]]
    ) -> list[CascadeResult]:
        """Build a dependency graph, find root nodes, and compute cascades.

        A root node is a table that no other table depends on.
        """
        # Collect all nodes
        all_nodes: set[str] = set(dependencies.keys())
        for deps in dependencies.values():
            all_nodes.update(deps)

        # Find non-root nodes (tables that appear as a dependency of something else)
        non_roots: set[str] = set()
        for deps in dependencies.values():
            non_roots.update(deps)

        roots = sorted(all_nodes - non_roots)

        # If there are no roots (e.g. circular), treat all source nodes as roots
        if not roots:
            roots = sorted(dependencies.keys())

        results: list[CascadeResult] = []
        for root in roots:
            dependents, depth = self._transitive_deps(root, dependencies)
            results.append(
                CascadeResult(
                    root_table=root,
                    dependent_tables=tuple(sorted(dependents)),
                    cascade_depth=depth,
                )
            )

        return results

    def get_co_migration_batches(
        self, dependencies: dict[str, list[str]]
    ) -> list[tuple[str, ...]]:
        """Return connected components — groups of tables that must migrate together."""
        # Build undirected adjacency list
        adjacency: dict[str, set[str]] = defaultdict(set)
        all_nodes: set[str] = set(dependencies.keys())
        for parent, children in dependencies.items():
            all_nodes.update(children)
            for child in children:
                adjacency[parent].add(child)
                adjacency[child].add(parent)

        visited: set[str] = set()
        components: list[tuple[str, ...]] = []

        for node in sorted(all_nodes):
            if node in visited:
                continue
            component: list[str] = []
            queue: deque[str] = deque([node])
            while queue:
                current = queue.popleft()
                if current in visited:
                    continue
                visited.add(current)
                component.append(current)
                for neighbour in sorted(adjacency[current]):
                    if neighbour not in visited:
                        queue.append(neighbour)
            components.append(tuple(sorted(component)))

        return components

    @staticmethod
    def _transitive_deps(
        root: str, dependencies: dict[str, list[str]]
    ) -> tuple[set[str], int]:
        """BFS from *root* following the dependency graph.

        Returns ``(all_dependents, max_depth)``.
        """
        visited: set[str] = set()
        queue: deque[tuple[str, int]] = deque([(root, 0)])
        max_depth = 0

        while queue:
            node, depth = queue.popleft()
            for child in dependencies.get(node, []):
                if child not in visited:
                    visited.add(child)
                    child_depth = depth + 1
                    max_depth = max(max_depth, child_depth)
                    queue.append((child, child_depth))

        return visited, max_depth
