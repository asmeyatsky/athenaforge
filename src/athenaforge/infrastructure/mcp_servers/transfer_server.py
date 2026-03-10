from __future__ import annotations

import json

from mcp.server import Server
from mcp.types import Resource, TextContent, Tool


def create_transfer_server(container) -> Server:
    """Create the transfer-forge MCP server.

    Wraps transfer use cases: Delta compaction planning, egress cost modelling,
    STS job creation, DVT validation, and streaming cutover control.
    """
    server = Server("transfer-forge")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="plan_delta_compaction",
                description="Evaluate Delta log health and produce compaction plans for tables",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "bucket": {
                            "type": "string",
                            "description": "Cloud storage bucket containing Delta tables",
                        },
                        "table_prefixes": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of table prefix paths to evaluate",
                        },
                    },
                    "required": ["bucket", "table_prefixes"],
                },
            ),
            Tool(
                name="model_egress_cost",
                description="Model data-egress costs across multiple scenarios",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "total_size_bytes": {
                            "type": "integer",
                            "description": "Total data size in bytes to transfer",
                        },
                        "credit_percentage": {
                            "type": "number",
                            "description": "Credit percentage to apply (0.0 to 100.0)",
                            "default": 0.0,
                        },
                    },
                    "required": ["total_size_bytes"],
                },
            ),
            Tool(
                name="create_sts_jobs",
                description="Create Storage Transfer Service jobs for source buckets",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "source_buckets": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of source bucket names to transfer from",
                        },
                        "dest_bucket": {
                            "type": "string",
                            "description": "Destination bucket name",
                        },
                    },
                    "required": ["source_buckets", "dest_bucket"],
                },
            ),
            Tool(
                name="run_dvt_validation",
                description="Run Data Validation Tool checks at a specified tier level",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tier": {
                            "type": "string",
                            "description": "Validation tier (tier1, tier2, or tier3)",
                            "enum": ["tier1", "tier2", "tier3"],
                        },
                        "table_pairs": {
                            "type": "array",
                            "items": {
                                "type": "array",
                                "items": {"type": "string"},
                                "minItems": 2,
                                "maxItems": 2,
                            },
                            "description": "List of [source_table, target_table] pairs",
                        },
                        "primary_keys": {
                            "type": "object",
                            "additionalProperties": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "description": "Map of source table to primary key columns",
                        },
                    },
                    "required": ["tier", "table_pairs"],
                },
            ),
            Tool(
                name="control_streaming_cutover",
                description="Orchestrate a streaming pipeline cutover from source to target topic",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "string",
                            "description": "Identifier for the streaming job",
                        },
                        "source_topic": {
                            "type": "string",
                            "description": "Source Kafka topic name",
                        },
                        "target_topic": {
                            "type": "string",
                            "description": "Target Kafka topic name",
                        },
                        "current_lag": {
                            "type": "integer",
                            "description": "Current consumer lag at cutover time",
                            "default": 0,
                        },
                    },
                    "required": ["job_id", "source_topic", "target_topic"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        try:
            if name == "plan_delta_compaction":
                result = await container.plan_delta_compaction_use_case.execute(
                    bucket=arguments["bucket"],
                    table_prefixes=arguments["table_prefixes"],
                )
                return [
                    TextContent(
                        type="text",
                        text=json.dumps([r.__dict__ for r in result]),
                    )
                ]

            if name == "model_egress_cost":
                result = await container.model_egress_cost_use_case.execute(
                    total_size_bytes=arguments["total_size_bytes"],
                    credit_percentage=arguments.get("credit_percentage", 0.0),
                )
                return [TextContent(type="text", text=json.dumps(result.__dict__))]

            if name == "create_sts_jobs":
                result = await container.create_sts_jobs_use_case.execute(
                    source_buckets=arguments["source_buckets"],
                    dest_bucket=arguments["dest_bucket"],
                )
                return [
                    TextContent(
                        type="text",
                        text=json.dumps([r.__dict__ for r in result]),
                    )
                ]

            if name == "run_dvt_validation":
                table_pairs = [
                    (pair[0], pair[1]) for pair in arguments["table_pairs"]
                ]
                result = await container.run_dvt_validation_use_case.execute(
                    tier=arguments["tier"],
                    table_pairs=table_pairs,
                    primary_keys=arguments.get("primary_keys"),
                )
                return [TextContent(type="text", text=json.dumps(result.__dict__))]

            if name == "control_streaming_cutover":
                result = await container.control_streaming_cutover_use_case.execute(
                    job_id=arguments["job_id"],
                    source_topic=arguments["source_topic"],
                    target_topic=arguments["target_topic"],
                    current_lag=arguments.get("current_lag", 0),
                )
                return [TextContent(type="text", text=json.dumps(result.__dict__))]

            raise ValueError(f"Unknown tool: {name}")
        except Exception as exc:
            return [TextContent(type="text", text=json.dumps({"error": str(exc)}))]

    @server.list_resources()
    async def list_resources() -> list[Resource]:
        return [
            Resource(
                uri="transfer://jobs",
                name="Transfer Jobs",
                description="List of all Storage Transfer Service jobs",
            ),
            Resource(
                uri="transfer://dvt-results/{tier}",
                name="DVT Results by Tier",
                description="Data Validation Tool results for a specific tier",
            ),
        ]

    @server.read_resource()
    async def read_resource(uri: str) -> str:
        if uri == "transfer://jobs":
            return json.dumps({"jobs": [], "status": "listing pending"})

        if uri.startswith("transfer://dvt-results/"):
            tier = uri.removeprefix("transfer://dvt-results/")
            return json.dumps({"tier": tier, "results": [], "status": "lookup pending"})

        raise ValueError(f"Unknown resource: {uri}")

    return server
