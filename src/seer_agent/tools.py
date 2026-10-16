from __future__ import annotations

from dataclasses import dataclass, field
import importlib.metadata
from typing import Any, Awaitable, Callable

from jsonschema import Draft202012Validator

from .contracts import ContractError, Event, ID, MemoryView, digest, encoded, validate


@dataclass(frozen=True)
class ToolSpec:
    name: str
    version: str
    effect: str
    input_schema: dict
    output_schema: dict
    configuration: dict = field(default_factory=dict)

    def fingerprint(self) -> str:
        validate(ID, self.name)
        if self.effect not in {"read", "local", "external"} or not self.version:
            raise ContractError("invalid_tool_descriptor")
        Draft202012Validator.check_schema(self.input_schema)
        Draft202012Validator.check_schema(self.output_schema)
        return digest(self.__dict__)


@dataclass(frozen=True)
class ToolContext:
    operation_id: str
    mission_id: str
    event: Event
    memory: MemoryView
    deadline_ms: int


@dataclass(frozen=True)
class ToolResult:
    status: str
    data: dict[str, Any] = field(default_factory=dict)
    receipt: str | None = None

    def validate(self, spec: ToolSpec) -> None:
        if self.status not in {"succeeded", "accepted", "failed", "unknown"}:
            raise ContractError("invalid_tool_status")
        if not isinstance(self.data, dict):
            raise ContractError("invalid_tool_data")
        if self.receipt is not None and (not isinstance(self.receipt, str) or len(self.receipt) > 512):
            raise ContractError("invalid_receipt")
        if self.status in {"succeeded", "accepted"}:
            validate(spec.output_schema, self.data)
        # Reserve room for provenance and execution metadata in the derived Event envelope.
        if len(encoded(self.data).encode()) > 65536:
            raise ContractError("tool_result_too_large")


class Tool:
    def __init__(self, spec: ToolSpec,
                 execute: Callable[[ToolContext, dict], Awaitable[ToolResult]]):
        self.spec = spec
        self.execute = execute
        spec.fingerprint()


class Registry:
    def __init__(self):
        self.tools: dict[str, Tool] = {}
        self.conditions: dict[str, Callable] = {}

    def tool(self, tool: Tool) -> None:
        if tool.spec.name in self.tools:
            raise ContractError("duplicate_tool")
        self.tools[tool.spec.name] = tool

    def condition(self, name: str, factory: Callable) -> None:
        validate(ID, name)
        if name in self.conditions:
            raise ContractError("duplicate_condition")
        self.conditions[name] = factory

    def load_plugin(self, name: str) -> None:
        """Load only an explicitly named installed plugin. This executes trusted code."""
        matches = list(importlib.metadata.entry_points(group="seer_agent.plugins", name=name))
        if len(matches) != 1:
            raise ContractError("plugin_missing_or_ambiguous")
        matches[0].load()(self)


def function_tool(name: str, *, input_schema: dict, output_schema: dict,
                  version: str = "1", effect: str = "read", configuration: dict | None = None):
    def decorate(function):
        return Tool(ToolSpec(name, version, effect, input_schema, output_schema, configuration or {}), function)
    return decorate
