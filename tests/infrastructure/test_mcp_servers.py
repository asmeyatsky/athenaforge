from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from mcp.server import Server
from mcp.types import ListResourcesRequest, ListToolsRequest

from athenaforge.infrastructure.mcp_servers import (
    create_dependency_server,
    create_foundation_server,
    create_sql_server,
    create_transfer_server,
    create_wave_server,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_container() -> MagicMock:
    """A mock container with no real use-case implementations.

    The smoke tests only exercise server construction and schema listing,
    so the container does not need working use-case attributes.
    """
    return MagicMock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SERVER_FACTORIES = [
    ("foundation", create_foundation_server),
    ("sql", create_sql_server),
    ("transfer", create_transfer_server),
    ("wave", create_wave_server),
    ("dependency", create_dependency_server),
]


# ---------------------------------------------------------------------------
# Tests — server creation
# ---------------------------------------------------------------------------


class TestServerCreation:
    """Each create_*_server function should return a Server instance."""

    @pytest.mark.parametrize("label,factory", _SERVER_FACTORIES)
    def test_returns_server_object(
        self, label: str, factory, mock_container: MagicMock
    ) -> None:
        server = factory(mock_container)
        assert isinstance(server, Server), (
            f"create_{label}_server did not return a Server instance"
        )


# ---------------------------------------------------------------------------
# Tests — tool schemas
# ---------------------------------------------------------------------------


class TestToolSchemas:
    """Tool definitions returned by list_tools must have name, description, and inputSchema."""

    async def test_foundation_tools_have_required_fields(
        self, mock_container: MagicMock
    ) -> None:
        server = create_foundation_server(mock_container)
        tools = await _get_tools(server)

        assert len(tools) > 0
        for tool in tools:
            _assert_tool_schema_valid(tool)

    async def test_sql_tools_have_required_fields(
        self, mock_container: MagicMock
    ) -> None:
        server = create_sql_server(mock_container)
        tools = await _get_tools(server)

        assert len(tools) > 0
        for tool in tools:
            _assert_tool_schema_valid(tool)

    async def test_transfer_tools_have_required_fields(
        self, mock_container: MagicMock
    ) -> None:
        server = create_transfer_server(mock_container)
        tools = await _get_tools(server)

        assert len(tools) > 0
        for tool in tools:
            _assert_tool_schema_valid(tool)

    async def test_wave_tools_have_required_fields(
        self, mock_container: MagicMock
    ) -> None:
        server = create_wave_server(mock_container)
        tools = await _get_tools(server)

        assert len(tools) > 0
        for tool in tools:
            _assert_tool_schema_valid(tool)

    async def test_dependency_tools_have_required_fields(
        self, mock_container: MagicMock
    ) -> None:
        server = create_dependency_server(mock_container)
        tools = await _get_tools(server)

        assert len(tools) > 0
        for tool in tools:
            _assert_tool_schema_valid(tool)


# ---------------------------------------------------------------------------
# Tests — resource definitions
# ---------------------------------------------------------------------------


class TestResourceDefinitions:
    """Resource definitions returned by list_resources should be well-formed."""

    async def test_foundation_resources_valid(
        self, mock_container: MagicMock
    ) -> None:
        server = create_foundation_server(mock_container)
        resources = await _get_resources(server)

        assert len(resources) > 0
        for r in resources:
            _assert_resource_valid(r)

    async def test_sql_resources_valid(
        self, mock_container: MagicMock
    ) -> None:
        server = create_sql_server(mock_container)
        resources = await _get_resources(server)

        assert len(resources) > 0
        for r in resources:
            _assert_resource_valid(r)

    async def test_transfer_resources_valid(
        self, mock_container: MagicMock
    ) -> None:
        server = create_transfer_server(mock_container)
        resources = await _get_resources(server)

        assert len(resources) > 0
        for r in resources:
            _assert_resource_valid(r)

    async def test_wave_resources_valid(
        self, mock_container: MagicMock
    ) -> None:
        server = create_wave_server(mock_container)
        resources = await _get_resources(server)

        assert len(resources) > 0
        for r in resources:
            _assert_resource_valid(r)

    async def test_dependency_resources_valid(
        self, mock_container: MagicMock
    ) -> None:
        server = create_dependency_server(mock_container)
        resources = await _get_resources(server)

        assert len(resources) > 0
        for r in resources:
            _assert_resource_valid(r)


# ---------------------------------------------------------------------------
# Tests — expected tool counts
# ---------------------------------------------------------------------------


class TestExpectedToolCounts:
    """Verify each server exposes the expected number of tools."""

    async def test_foundation_has_5_tools(self, mock_container: MagicMock) -> None:
        server = create_foundation_server(mock_container)
        tools = await _get_tools(server)
        assert len(tools) == 5

    async def test_sql_has_5_tools(self, mock_container: MagicMock) -> None:
        server = create_sql_server(mock_container)
        tools = await _get_tools(server)
        assert len(tools) == 5

    async def test_transfer_has_5_tools(self, mock_container: MagicMock) -> None:
        server = create_transfer_server(mock_container)
        tools = await _get_tools(server)
        assert len(tools) == 5

    async def test_wave_has_6_tools(self, mock_container: MagicMock) -> None:
        server = create_wave_server(mock_container)
        tools = await _get_tools(server)
        assert len(tools) == 6

    async def test_dependency_has_5_tools(self, mock_container: MagicMock) -> None:
        server = create_dependency_server(mock_container)
        tools = await _get_tools(server)
        assert len(tools) == 5


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _get_tools(server: Server) -> list:
    """Invoke the registered list_tools handler on *server*."""
    handler = server.request_handlers.get(ListToolsRequest)
    assert handler is not None, "Server has no ListToolsRequest handler registered"
    result = await handler(ListToolsRequest(method="tools/list"))
    return result.root.tools


async def _get_resources(server: Server) -> list:
    """Invoke the registered list_resources handler on *server*."""
    handler = server.request_handlers.get(ListResourcesRequest)
    assert handler is not None, "Server has no ListResourcesRequest handler registered"
    result = await handler(ListResourcesRequest(method="resources/list"))
    return result.root.resources


def _assert_tool_schema_valid(tool) -> None:
    """Assert that a Tool object has name, description, and inputSchema."""
    assert tool.name, f"Tool has empty name: {tool}"
    assert tool.description, f"Tool '{tool.name}' has empty description"
    schema = tool.inputSchema
    assert isinstance(schema, dict), (
        f"Tool '{tool.name}' inputSchema is not a dict: {type(schema)}"
    )
    assert "type" in schema, (
        f"Tool '{tool.name}' inputSchema missing 'type' key"
    )
    assert "properties" in schema, (
        f"Tool '{tool.name}' inputSchema missing 'properties' key"
    )


def _assert_resource_valid(resource) -> None:
    """Assert that a Resource object has uri, name, and description."""
    assert resource.uri, f"Resource has empty uri: {resource}"
    assert resource.name, f"Resource '{resource.uri}' has empty name"
    assert resource.description, (
        f"Resource '{resource.uri}' has empty description"
    )
