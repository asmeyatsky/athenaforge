from __future__ import annotations

import json

from mcp.server import Server
from mcp.types import Resource, TextContent, Tool


def create_foundation_server(container) -> Server:
    """Create the foundation-forge MCP server.

    Wraps foundation use cases: scaffold generation, tier classification,
    pricing configuration, Dataplex bootstrap, and Delta health checks.
    """
    server = Server("foundation-forge")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="generate_scaffold",
                description="Generate Terraform scaffold files for each LOB in a manifest",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "manifest_path": {
                            "type": "string",
                            "description": "Path to the LOB manifest file",
                        },
                        "output_dir": {
                            "type": "string",
                            "description": "Directory to write generated Terraform files",
                        },
                    },
                    "required": ["manifest_path", "output_dir"],
                },
            ),
            Tool(
                name="classify_tiers",
                description="Classify all tables in an inventory into migration tiers",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "inventory_id": {
                            "type": "string",
                            "description": "Identifier for the table inventory to classify",
                        },
                    },
                    "required": ["inventory_id"],
                },
            ),
            Tool(
                name="configure_pricing",
                description="Calculate slot pricing and generate a Terraform reservation file",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "slots": {
                            "type": "integer",
                            "description": "Number of BigQuery slots to reserve",
                        },
                        "commitment_years": {
                            "type": "integer",
                            "description": "Commitment period in years (1 or 3)",
                        },
                        "output_dir": {
                            "type": "string",
                            "description": "Directory to write the reservation Terraform file",
                        },
                    },
                    "required": ["slots", "commitment_years", "output_dir"],
                },
            ),
            Tool(
                name="bootstrap_dataplex",
                description="Bootstrap a Dataplex lake with specified zones",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "lake_name": {
                            "type": "string",
                            "description": "Name for the Dataplex lake",
                        },
                        "zones": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of zone names to create",
                        },
                    },
                    "required": ["lake_name", "zones"],
                },
            ),
            Tool(
                name="check_delta_health",
                description="Check Delta transaction log health for tables under a bucket prefix",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "bucket": {
                            "type": "string",
                            "description": "Cloud storage bucket name",
                        },
                        "table_prefix": {
                            "type": "string",
                            "description": "Prefix path for tables to check",
                        },
                    },
                    "required": ["bucket", "table_prefix"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        try:
            if name == "generate_scaffold":
                result = await container.generate_scaffold_use_case.execute(
                    manifest_path=arguments["manifest_path"],
                    output_dir=arguments["output_dir"],
                )
                return [TextContent(type="text", text=json.dumps(result.__dict__))]

            if name == "classify_tiers":
                result = await container.classify_tiers_use_case.execute(
                    inventory_id=arguments["inventory_id"],
                )
                return [TextContent(type="text", text=json.dumps(result.__dict__))]

            if name == "configure_pricing":
                result = await container.configure_pricing_use_case.execute(
                    slots=arguments["slots"],
                    commitment_years=arguments["commitment_years"],
                    output_dir=arguments["output_dir"],
                )
                return [TextContent(type="text", text=json.dumps(result.__dict__))]

            if name == "bootstrap_dataplex":
                result = await container.bootstrap_dataplex_use_case.execute(
                    lake_name=arguments["lake_name"],
                    zones=arguments["zones"],
                )
                return [TextContent(type="text", text=json.dumps(result))]

            if name == "check_delta_health":
                result = await container.check_delta_health_use_case.execute(
                    bucket=arguments["bucket"],
                    table_prefix=arguments["table_prefix"],
                )
                return [
                    TextContent(
                        type="text",
                        text=json.dumps([r.__dict__ for r in result]),
                    )
                ]

            raise ValueError(f"Unknown tool: {name}")
        except Exception as exc:
            return [TextContent(type="text", text=json.dumps({"error": str(exc)}))]

    @server.list_resources()
    async def list_resources() -> list[Resource]:
        return [
            Resource(
                uri="foundation://tier-summary",
                name="Tier Summary",
                description="Aggregated tier classification summary for the current inventory",
            ),
            Resource(
                uri="foundation://scaffold/{lob}",
                name="LOB Scaffold",
                description="Generated Terraform scaffold for a specific LOB",
            ),
        ]

    @server.read_resource()
    async def read_resource(uri: str) -> str:
        if uri == "foundation://tier-summary":
            result = await container.get_tier_summary_query.execute(
                inventory_id="default",
            )
            return json.dumps(result.__dict__)

        if uri.startswith("foundation://scaffold/"):
            lob = uri.removeprefix("foundation://scaffold/")
            return json.dumps({"lob": lob, "status": "scaffold available"})

        raise ValueError(f"Unknown resource: {uri}")

    return server
