from __future__ import annotations

import json

from mcp.server import Server
from mcp.types import Resource, TextContent, Tool


def create_dependency_server(container) -> Server:
    """Create the dependency-forge MCP server.

    Wraps dependency use cases: Spark/Flink job scanning, DAG rewriting,
    Kafka topic migration, Lambda rewriting, and IAM permission mapping.
    """
    server = Server("dependency-forge")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="scan_spark_flink_jobs",
                description="Scan cloud storage for Spark/Flink job files and identify dependencies",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "bucket": {
                            "type": "string",
                            "description": "Cloud storage bucket to scan",
                        },
                        "prefixes": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of key prefixes to scan for job files",
                        },
                    },
                    "required": ["bucket", "prefixes"],
                },
            ),
            Tool(
                name="rewrite_dags",
                description="Rewrite Airflow DAGs from AWS operators to GCP equivalents",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "dag_contents": {
                            "type": "object",
                            "additionalProperties": {"type": "string"},
                            "description": "Map of DAG file path to file content",
                        },
                    },
                    "required": ["dag_contents"],
                },
            ),
            Tool(
                name="migrate_kafka_topics",
                description="Migrate Kafka topic configurations to the target platform",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "topic_configs": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "topic": {
                                        "type": "string",
                                        "description": "Kafka topic name",
                                    },
                                    "schema": {
                                        "type": "string",
                                        "description": "Schema reference for the topic",
                                    },
                                },
                                "required": ["topic"],
                            },
                            "description": "List of Kafka topic configurations",
                        },
                    },
                    "required": ["topic_configs"],
                },
            ),
            Tool(
                name="rewrite_lambdas",
                description="Scan Lambda function sources for Athena references and mark for rewrite",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "lambda_sources": {
                            "type": "object",
                            "additionalProperties": {"type": "string"},
                            "description": "Map of function name to source code",
                        },
                    },
                    "required": ["lambda_sources"],
                },
            ),
            Tool(
                name="map_iam_permissions",
                description="Map Lake Formation policies to BigQuery IAM role equivalents",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "lake_formation_policies": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "permission": {
                                        "type": "string",
                                        "description": "Lake Formation permission (SELECT, INSERT, UPDATE, DELETE, ALL)",
                                    },
                                    "resource": {
                                        "type": "string",
                                        "description": "Resource the permission applies to",
                                    },
                                    "principal": {
                                        "type": "string",
                                        "description": "IAM principal (user or role ARN)",
                                    },
                                },
                                "required": ["permission", "resource", "principal"],
                            },
                            "description": "List of Lake Formation policy entries to map",
                        },
                    },
                    "required": ["lake_formation_policies"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        if name == "scan_spark_flink_jobs":
            result = await container.scan_spark_flink_jobs_use_case.execute(
                bucket=arguments["bucket"],
                prefixes=arguments["prefixes"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "rewrite_dags":
            result = await container.rewrite_dags_use_case.execute(
                dag_contents=arguments["dag_contents"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "migrate_kafka_topics":
            result = await container.migrate_kafka_topics_use_case.execute(
                topic_configs=arguments["topic_configs"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "rewrite_lambdas":
            result = await container.rewrite_lambdas_use_case.execute(
                lambda_sources=arguments["lambda_sources"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        if name == "map_iam_permissions":
            result = await container.map_iam_permissions_use_case.execute(
                lake_formation_policies=arguments["lake_formation_policies"],
            )
            return [TextContent(type="text", text=json.dumps(result.__dict__))]

        raise ValueError(f"Unknown tool: {name}")

    @server.list_resources()
    async def list_resources() -> list[Resource]:
        return [
            Resource(
                uri="dependency://scan-report",
                name="Dependency Scan Report",
                description="Latest Spark/Flink dependency scan report",
            ),
            Resource(
                uri="dependency://dag-status",
                name="DAG Rewrite Status",
                description="Status of Airflow DAG rewrites",
            ),
        ]

    @server.read_resource()
    async def read_resource(uri: str) -> str:
        if uri == "dependency://scan-report":
            return json.dumps({"report": "scan report pending"})

        if uri == "dependency://dag-status":
            return json.dumps({"dag_status": "rewrite status pending"})

        raise ValueError(f"Unknown resource: {uri}")

    return server
