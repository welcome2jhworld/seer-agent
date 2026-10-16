# Extension contracts

Use trusted Python registration to add a tool. The core does not need provider-specific branches.

```python
import asyncio
from seer_agent import ToolResult, function_tool
from seer_agent.builtins import DETECTIONS

def detector_tool(model):
    @function_tool("site.detect", input_schema={"type": "object", "additionalProperties": False},
                   output_schema=DETECTIONS, effect="read", version="1")
    async def detect(context, arguments):
        # The host resolves and authorizes the original frame reference.
        detections = await asyncio.to_thread(model.detect, context.event.data["frame_ref"])
        return ToolResult("succeeded", {"detections": detections})
    return detect
```

`ToolContext` contains the original operation identity, mission version, source event, pinned memory and deadline. Use that operation identity as a provider idempotency key where the provider actually supports one. Return `succeeded` for a verified local/read result, `accepted` for provider acceptance with its receipt, `failed` only for a known failure, and `unknown` when the effect cannot be established. Runtime exceptions are deliberately reduced to a generic unknown outcome to avoid leaking credentials or assuming no effect.

`ToolSpec` includes JSON Schemas, effect class, semantic version and non-secret configuration. Configuration must identify anything that changes an effect, including endpoint, destination, model version and credential environment identity. Secret values stay outside descriptors and mission files. Changing code behavior without changing the version is a plugin author error that fingerprinting cannot detect.

Conditions expose a `version` and `evaluate(event, state, memory) -> Evaluation`. Their returned state must be JSON data. Evidence must refer to the current observation or records in the scoped memory snapshot. A transition selects only the actions already present in the approved Mission. A condition cannot register tools, widen policy or select an arbitrary recipient. Custom condition code is trusted code and must be deterministic and fast.

Sources yield `Event` values. Stable IDs should contain session and sequence or a provider event ID; timestamps alone are not identities. `sources.frames` adapts an async camera reader's references, `sources.document` reads a bounded UTF-8 document, and `sources.message` creates a chat event. They do not execute text as instructions or infer structured operator facts automatically. Add an explicitly approved interpretation tool when a workflow needs that transformation.

Only the two workflows in `examples/` are built-in examples. The person example includes a separately installable plugin demonstrating a custom detector and condition. Package integrations with:

```toml
[project.entry-points."seer_agent.plugins"]
site = "site_tools:register"
```

Load it with `seer-agent run ... --plugin site`. Entry points are not loaded automatically. Installing/loading a plugin executes Python code with the host's privileges; it is not a sandbox. Model output and incoming events cannot name plugins to load.
