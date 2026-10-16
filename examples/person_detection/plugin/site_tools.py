"""An installed third-party package, independent of the harness source tree."""
from seer_agent import Evaluation, ToolResult, function_tool
from seer_agent.builtins import DETECTIONS
from seer_agent.conditions import Presence


@function_tool("site.detect", input_schema={"type": "object", "additionalProperties": False},
               output_schema=DETECTIONS, configuration={"synthetic": True, "model": "site-fixture-v1"})
async def detect(context, arguments):
    if context.event.provenance != "synthetic":
        return ToolResult("unknown", {"reason": "sample_requires_synthetic_data"})
    # Replace this fixture with your model SDK; return the same detections schema.
    return ToolResult("succeeded", context.event.data["fixture_result"])


class SitePresence(Presence):
    version = "site-1"

    def evaluate(self, event, state, memory):
        result = super().evaluate(event, state, memory)
        if result.transition == "opened":
            return Evaluation(result.state, "review", "Site policy requires an operator review of this presence event.", result.evidence)
        return result


def register(registry):
    registry.tool(detect)
    registry.condition("site.presence", SitePresence)
