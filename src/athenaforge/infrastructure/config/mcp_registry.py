from __future__ import annotations

from typing import Any

from athenaforge.infrastructure.config.container import DependencyContainer


class MCPServerRegistry:
    """Registry for MCP (Model Context Protocol) servers.

    Provides lifecycle management for all registered servers: registration,
    start, stop, and retrieval by name.
    """

    def __init__(self) -> None:
        self._servers: dict[str, Any] = {}
        self._running: set[str] = set()

    def register_all(self, container: DependencyContainer) -> None:
        """Register all five MCP servers using the wired container.

        Each server is stored as a dict containing the server name,
        description, and the set of use cases it exposes.
        """
        self._servers["foundation"] = {
            "name": "foundation",
            "description": "Foundation MCP server for scaffold, tiers, pricing, and Dataplex",
            "use_cases": {
                "generate_scaffold": container.generate_scaffold_use_case,
                "classify_tiers": container.classify_tiers_use_case,
                "configure_pricing": container.configure_pricing_use_case,
                "bootstrap_dataplex": container.bootstrap_dataplex_use_case,
                "check_delta_health": container.check_delta_health_use_case,
                "get_tier_summary": container.get_tier_summary_query,
            },
        }

        self._servers["sql"] = {
            "name": "sql",
            "description": "SQL translation MCP server for batch translation and analysis",
            "use_cases": {
                "translate_batch": container.translate_batch_use_case,
                "analyse_map_cascade": container.analyse_map_cascade_use_case,
                "normalise_case_sensitivity": container.normalise_case_sensitivity_use_case,
                "classify_udfs": container.classify_udfs_use_case,
                "validate_queries": container.validate_queries_use_case,
            },
        }

        self._servers["transfer"] = {
            "name": "transfer",
            "description": "Data transfer MCP server for STS, DVT, egress, and streaming",
            "use_cases": {
                "plan_delta_compaction": container.plan_delta_compaction_use_case,
                "model_egress_cost": container.model_egress_cost_use_case,
                "create_sts_jobs": container.create_sts_jobs_use_case,
                "run_dvt_validation": container.run_dvt_validation_use_case,
                "control_streaming_cutover": container.control_streaming_cutover_use_case,
            },
        }

        self._servers["wave"] = {
            "name": "wave",
            "description": "Wave management MCP server for planning, gates, and rollback",
            "use_cases": {
                "plan_waves": container.plan_waves_use_case,
                "evaluate_rollback": container.evaluate_rollback_use_case,
                "enforce_wave_gate": container.enforce_wave_gate_use_case,
                "migrate_dashboards": container.migrate_dashboards_use_case,
                "reconcile_kpis": container.reconcile_kpis_use_case,
                "control_parallel_run": container.control_parallel_run_use_case,
            },
        }

        self._servers["dependency"] = {
            "name": "dependency",
            "description": "Dependency management MCP server for scanning, rewriting, and IAM mapping",
            "use_cases": {
                "scan_spark_flink_jobs": container.scan_spark_flink_jobs_use_case,
                "rewrite_dags": container.rewrite_dags_use_case,
                "migrate_kafka_topics": container.migrate_kafka_topics_use_case,
                "rewrite_lambdas": container.rewrite_lambdas_use_case,
                "map_iam_permissions": container.map_iam_permissions_use_case,
            },
        }

    def start_all(self) -> None:
        """Start all registered servers."""
        for name in self._servers:
            self._running.add(name)

    def stop_all(self) -> None:
        """Stop all running servers."""
        self._running.clear()

    def get_server(self, name: str) -> Any:
        """Return a registered server by name.

        Raises ``KeyError`` if the server is not registered.
        """
        if name not in self._servers:
            raise KeyError(f"MCP server '{name}' is not registered")
        return self._servers[name]

    @property
    def registered_names(self) -> list[str]:
        """Return sorted list of all registered server names."""
        return sorted(self._servers.keys())

    @property
    def running_names(self) -> list[str]:
        """Return sorted list of all currently running server names."""
        return sorted(self._running)
