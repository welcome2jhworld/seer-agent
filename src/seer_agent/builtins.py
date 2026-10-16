from .conditions import register_conditions
from .contracts import ID, TEXT, object_schema
from .tools import Registry, Tool, ToolResult, ToolSpec


DETECTIONS = object_schema({"detections": {"type": "array", "maxItems": 1000, "items": object_schema({
    "label": {"type": "string"}, "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    "box": {"type": "array", "minItems": 4, "maxItems": 4,
            "items": {"type": "number", "minimum": 0, "maximum": 1}},
})}})
CHANGE = object_schema({"change": {"enum": ["changed", "unchanged", "unknown"]},
    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    "reference_id": ID})


def builtin_registry() -> Registry:
    registry = Registry()
    register_conditions(registry)
    async def record(context, arguments):
        # The committed tool result itself is the local record; there is no second file effect.
        return ToolResult("succeeded", {"text": arguments["text"], "evidence": list(context.event.evidence)}, context.operation_id)
    registry.tool(Tool(ToolSpec("record", "1", "local", object_schema({"text": TEXT}),
        object_schema({"text": TEXT, "evidence": {"type": "array", "items": {"type": "string"}}})), record))
    return registry


def demo_registry() -> Registry:
    registry = builtin_registry()
    for name, output in [("fixture.detect", DETECTIONS), ("fixture.compare", CHANGE)]:
        async def execute(context, arguments):
            if context.event.provenance != "synthetic":
                return ToolResult("failed", {"reason": "fixture_requires_synthetic_input"})
            if context.event.quality != "known":
                return ToolResult("unknown", {"reason": "fixture_input_unknown"})
            return ToolResult("succeeded", context.event.data["fixture_result"])
        registry.tool(Tool(ToolSpec(name, "1", "read", object_schema({}), output, {"synthetic": True}), execute))
    return registry
