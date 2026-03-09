from __future__ import annotations

import json

from mcp.server import Server
from mcp.types import Resource, TextContent, Tool


def create_sql_server(container) -> Server:
    """Create the sql-forge MCP server.

    Wraps SQL use cases: batch translation, map-cascade analysis,
    case-sensitivity normalisation, UDF classification, and query validation.
    """
    server = Server("sql-forge")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="translate_batch",
                description="Translate a batch of SQL files from source dialect to BigQuery SQL",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "source_dir": {
                            "type": "string",
                            "description": "Directory containing source SQL files",
                        },
                        "output_dir": {
                            "type": "string",
                            "description": "Directory to write translated SQL files",
                        },
                    },
                    "required": ["source_dir", "output_dir"],
                },
            ),
            Tool(
                name="analyse_map_cascade",
                description="Analyse dependency cascades and co-migration batches for table mappings",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "dependencies": {
                            "type": "object",
                            "additionalProperties": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "description": "Map of table name to list of dependent table names",
                        },
                    },
                    "required": ["dependencies"],
                },
            ),
            Tool(
                name="normalise_case_sensitivity",
                description="Wrap column references in UPPER() for case-insensitive comparison",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sql_content": {
                            "type": "string",
                            "description": "SQL content to normalise",
                        },
                        "columns": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Column names to wrap in UPPER()",
                        },
                    },
                    "required": ["sql_content", "columns"],
                },
            ),
            Tool(
                name="classify_udfs",
                description="Classify UDFs into SQL, JavaScript, or Cloud Run categories",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "udfs": {
                            "type": "object",
                            "additionalProperties": {"type": "string"},
                            "description": "Map of UDF name to UDF body source code",
                        },
                    },
                    "required": ["udfs"],
                },
            ),
            Tool(
                name="validate_queries",
                description="Dry-run translated queries against BigQuery to validate correctness",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query_paths": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of query file paths to validate",
                        },
                        "query_contents": {
                            "type": "object",
                            "additionalProperties": {"type": "string"},
                            "description": "Map of query path to SQL content",
                        },
                    },
                    "required": ["query_paths", "query_contents"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        if name == "translate_batch":
            result = await container.translate_batch_use_case.execute(
                source_dir=arguments["source_dir"],
                output_dir=arguments["output_dir"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "analyse_map_cascade":
            result = await container.analyse_map_cascade_use_case.execute(
                dependencies=arguments["dependencies"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "normalise_case_sensitivity":
            result = await container.normalise_case_sensitivity_use_case.execute(
                sql_content=arguments["sql_content"],
                columns=arguments["columns"],
            )
            return [TextContent(type="text", text=result)]

        if name == "classify_udfs":
            result = await container.classify_udfs_use_case.execute(
                udfs=arguments["udfs"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "validate_queries":
            result = await container.validate_queries_use_case.execute(
                query_paths=arguments["query_paths"],
                query_contents=arguments["query_contents"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        raise ValueError(f"Unknown tool: {name}")

    @server.list_resources()
    async def list_resources() -> list[Resource]:
        return [
            Resource(
                uri="sql://batch/{id}",
                name="Translation Batch",
                description="Details of a specific SQL translation batch by ID",
            ),
            Resource(
                uri="sql://patterns",
                name="SQL Patterns",
                description="Available SQL pattern rules used during translation",
            ),
        ]

    @server.read_resource()
    async def read_resource(uri: str) -> str:
        if uri.startswith("sql://batch/"):
            batch_id = uri.removeprefix("sql://batch/")
            return json.dumps({"batch_id": batch_id, "status": "lookup pending"})

        if uri == "sql://patterns":
            return json.dumps({"patterns": "pattern catalogue available"})

        raise ValueError(f"Unknown resource: {uri}")

    return server
