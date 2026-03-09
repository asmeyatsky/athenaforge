from __future__ import annotations

import json

from mcp.server import Server
from mcp.types import Resource, TextContent, Tool


def create_wave_server(container) -> Server:
    """Create the wave-forge MCP server.

    Wraps wave use cases: wave planning, parallel-run control, rollback
    evaluation, wave gate enforcement, dashboard migration, and KPI
    reconciliation.
    """
    server = Server("wave-forge")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="plan_waves",
                description="Plan migration waves for a given inventory and set of LOBs",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "inventory_id": {
                            "type": "string",
                            "description": "Identifier for the table inventory",
                        },
                        "lobs": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of LOB names to include in wave planning",
                        },
                        "max_parallel": {
                            "type": "integer",
                            "description": "Maximum number of parallel waves",
                            "default": 3,
                        },
                    },
                    "required": ["inventory_id", "lobs"],
                },
            ),
            Tool(
                name="control_parallel_run",
                description="Transition a wave's parallel-run mode using the state machine",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "wave_id": {
                            "type": "string",
                            "description": "Identifier for the wave",
                        },
                        "target_mode": {
                            "type": "string",
                            "description": "Target parallel-run mode to transition to",
                        },
                    },
                    "required": ["wave_id", "target_mode"],
                },
            ),
            Tool(
                name="evaluate_rollback",
                description="Evaluate whether a wave should be rolled back based on operational metrics",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "wave_id": {
                            "type": "string",
                            "description": "Identifier for the wave to evaluate",
                        },
                        "dvt_pass_rate": {
                            "type": "number",
                            "description": "DVT validation pass rate (0.0 to 1.0)",
                        },
                        "latency_increase_pct": {
                            "type": "number",
                            "description": "Percentage increase in latency",
                        },
                        "data_loss_detected": {
                            "type": "boolean",
                            "description": "Whether data loss has been detected",
                        },
                        "streaming_lag": {
                            "type": "integer",
                            "description": "Current streaming consumer lag",
                        },
                        "escalation_raised": {
                            "type": "boolean",
                            "description": "Whether an escalation has been raised",
                        },
                    },
                    "required": [
                        "wave_id",
                        "dvt_pass_rate",
                        "latency_increase_pct",
                        "data_loss_detected",
                        "streaming_lag",
                        "escalation_raised",
                    ],
                },
            ),
            Tool(
                name="enforce_wave_gate",
                description="Enforce the quality gate for a wave by checking all required criteria",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "wave_id": {
                            "type": "string",
                            "description": "Identifier for the wave",
                        },
                        "criteria": {
                            "type": "object",
                            "additionalProperties": {"type": "boolean"},
                            "description": "Map of criterion name to pass/fail boolean",
                        },
                    },
                    "required": ["wave_id", "criteria"],
                },
            ),
            Tool(
                name="migrate_dashboards",
                description="Migrate dashboards from the legacy platform to the new platform",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "dashboard_configs": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "description": "Dashboard name",
                                    },
                                },
                                "required": ["name"],
                            },
                            "description": "List of dashboard configurations to migrate",
                        },
                    },
                    "required": ["dashboard_configs"],
                },
            ),
            Tool(
                name="reconcile_kpis",
                description="Reconcile KPIs between legacy and new platform",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "kpi_definitions": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "description": "KPI name",
                                    },
                                },
                                "required": ["name"],
                            },
                            "description": "List of KPI definitions to reconcile",
                        },
                    },
                    "required": ["kpi_definitions"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        if name == "plan_waves":
            result = await container.plan_waves_use_case.execute(
                inventory_id=arguments["inventory_id"],
                lobs=arguments["lobs"],
                max_parallel=arguments.get("max_parallel", 3),
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "control_parallel_run":
            result = await container.control_parallel_run_use_case.execute(
                wave_id=arguments["wave_id"],
                target_mode=arguments["target_mode"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "evaluate_rollback":
            result = await container.evaluate_rollback_use_case.execute(
                wave_id=arguments["wave_id"],
                dvt_pass_rate=arguments["dvt_pass_rate"],
                latency_increase_pct=arguments["latency_increase_pct"],
                data_loss_detected=arguments["data_loss_detected"],
                streaming_lag=arguments["streaming_lag"],
                escalation_raised=arguments["escalation_raised"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "enforce_wave_gate":
            result = await container.enforce_wave_gate_use_case.execute(
                wave_id=arguments["wave_id"],
                criteria=arguments["criteria"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "migrate_dashboards":
            result = await container.migrate_dashboards_use_case.execute(
                dashboard_configs=arguments["dashboard_configs"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "reconcile_kpis":
            result = await container.reconcile_kpis_use_case.execute(
                kpi_definitions=arguments["kpi_definitions"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        raise ValueError(f"Unknown tool: {name}")

    @server.list_resources()
    async def list_resources() -> list[Resource]:
        return [
            Resource(
                uri="wave://plan",
                name="Wave Plan",
                description="Current migration wave plan overview",
            ),
            Resource(
                uri="wave://waves/{id}",
                name="Wave Details",
                description="Details for a specific wave by ID",
            ),
            Resource(
                uri="wave://gantt",
                name="Wave Gantt",
                description="Gantt chart data for wave timeline visualisation",
            ),
        ]

    @server.read_resource()
    async def read_resource(uri: str) -> str:
        if uri == "wave://plan":
            return json.dumps({"plan": "wave plan data pending"})

        if uri.startswith("wave://waves/"):
            wave_id = uri.removeprefix("wave://waves/")
            return json.dumps({"wave_id": wave_id, "status": "lookup pending"})

        if uri == "wave://gantt":
            return json.dumps({"gantt": "timeline data pending"})

        raise ValueError(f"Unknown resource: {uri}")

    return server
