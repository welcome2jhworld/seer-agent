from __future__ import annotations

import asyncio
import json
import time
from typing import Callable

from .contracts import ContractError, Event, Evaluation, MemoryView, Mission, digest, encoded, validate
from .store import Store
from .tools import Registry, ToolContext, ToolResult


def wall_time_ms() -> int:
    return time.time_ns() // 1_000_000


class Runtime:
    def __init__(self, mission: Mission, registry: Registry, store: Store,
                 *, clock: Callable[[], int] = wall_time_ms, allow_external: bool = False):
        self.mission = Mission.from_dict(mission.document)
        self.registry, self.store, self.clock = registry, store, clock
        self.allow_external = allow_external
        self.key = self.mission.key
        self.doc = self.mission.document
        self._definition_hash = digest(self.doc)
        name = self.doc["condition"]["type"]
        if name not in registry.conditions:
            raise ContractError("condition_not_registered")
        self.condition = registry.conditions[name](self.doc["condition"]["config"])
        names = {r["tool"] for r in self.doc["observe"] + self.doc["act"]}
        if not names.issubset(registry.tools):
            raise ContractError("tool_not_registered")
        self.fingerprints = {n: registry.tools[n].spec.fingerprint() for n in sorted(names)}
        for route in self.doc["observe"]:
            if registry.tools[route["tool"]].spec.effect != "read":
                raise ContractError("observation_tools_must_be_read_only")
        store.open_mission(self.key, {"mission": self.doc, "condition_version": self.condition.version,
                                     "tools": self.fingerprints})
        self._dispatch_lock = asyncio.Lock()

    @property
    def state(self) -> dict:
        return self.store.state(self.key)

    def memory(self, now: int | None = None) -> MemoryView:
        config = self.doc["remember"]
        return self.store.memory(self.key, config["sources"], now=self.clock() if now is None else now,
                                 max_age=config["max_age_ms"], limit=config["max_items"])

    def _denial(self, name: str) -> str | None:
        tool = self.registry.tools.get(name)
        policy = self.doc["policy"]
        if tool is None or tool.spec.fingerprint() != self.fingerprints.get(name):
            return "tool_definition_changed"
        if name not in policy["allowed_tools"]:
            return "tool_not_allowed"
        if tool.spec.effect == "external" and not (policy["allow_external"] and self.allow_external):
            return "external_effect_disabled"
        return None

    def _check_definition(self):
        if digest(self.doc) != self._definition_hash:
            raise ContractError("mission_mutated_without_new_version")

    def _plan(self, event: Event, role: str, index: int, binding: dict,
              memory: MemoryView, now: int, evaluation: Evaluation | None = None):
        name = binding["tool"]
        args = json.loads(encoded(binding.get("arguments", {})))
        if evaluation:
            replacements = {"$summary": evaluation.summary, "$event_id": event.id,
                            "$mission_id": self.key, "$evidence": list(evaluation.evidence)}
            args = {k: replacements.get(v, v) if isinstance(v, str) else v for k, v in args.items()}
        # Model tools receive the source event via ToolContext; arguments remain host configuration.
        validate(self.registry.tools[name].spec.input_schema, args)
        operation = digest([self.key, event.id, role, index, name])
        denial = self._denial(name)
        depth_exhausted = self.store.event(self.key, event.id)["depth"] >= self.doc["policy"]["max_tool_depth"]
        if depth_exhausted:
            denial = "tool_depth_exhausted"
        status = "denied" if denial else "pending"
        self.store.db.execute("""INSERT INTO operations
            (id,mission,event_id,event_revision,role,tool,fingerprint,arguments,memory_ids,emits,status,created,deadline,result)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                operation, self.key, event.id, self.store.event(self.key, event.id)["revision"], role, name, self.fingerprints[name], encoded(args),
                encoded([r.event.id for r in memory.query(limit=1000)]), None if depth_exhausted else binding.get("emits"), status, now,
                now+self.doc["policy"]["timeout_ms"], encoded({"reason": denial}) if denial else None))
        self.store.db.execute("INSERT INTO operation_history VALUES(NULL,?,?,?,?)", (operation, status, now, "{}"))

    def ingest(self, event: Event, *, _derived: bool = False, _available: int | None = None, _depth: int = 0) -> dict:
        self._check_definition()
        event = Event.from_dict(event.to_dict())
        if event.id.startswith("tool:") and not _derived:
            raise ContractError("reserved_event_identity")
        now = self.clock()
        with self.store.transaction():
            previous = self.store.event(self.key, event.id)
            if previous:
                if previous["body"] != encoded(event.to_dict()):
                    raise ContractError("event_identity_conflict")
                return {"status": "duplicate", "event_id": event.id}
            if now < self.store.clock(self.key):
                raise ContractError("clock_moved_backwards")
            available = now if _available is None else _available
            if available > now:
                raise ContractError("future_availability")
            allowed = event.source in self.doc["sources"] and (
                _derived or event.kind in self.doc["sources"][event.source])
            timely = event.observed_at_ms <= available and now-event.observed_at_ms <= self.doc["policy"]["max_event_age_ms"]
            admitted = allowed and timely
            self.store.db.execute("""INSERT INTO events(mission,id,source,kind,observed,available,body,admitted,depth)
                VALUES(?,?,?,?,?,?,?,?,?)""", (self.key, event.id, event.source, event.kind, event.observed_at_ms,
                available, encoded(event.to_dict()), int(admitted), _depth))
            if not admitted:
                return {"status": "rejected", "event_id": event.id, "reason": "source_or_time_policy"}
            memory = self.memory(now)
            old_state = self.store.state(self.key)
            try:
                result = self.condition.evaluate(event, old_state, memory)
            except Exception:
                raise ContractError("condition_failed") from None
            if not isinstance(result, Evaluation) or len(encoded(result.state).encode()) > 65536:
                raise ContractError("invalid_condition_result")
            allowed_evidence = {event.id, *(r.event.id for r in memory.query(limit=1000))}
            if not set(result.evidence).issubset(allowed_evidence) or len(result.evidence) > 64:
                raise ContractError("condition_invented_evidence")
            self.store.db.execute("UPDATE missions SET state=?,clock=? WHERE id=?", (encoded(result.state), now, self.key))
            self.store.db.execute("INSERT INTO decisions VALUES(NULL,?,?,?,?,?,?)", (
                self.key, event.id, result.transition, result.summary, encoded(result.evidence), encoded(result.state)))
            if not _derived:
                for index, binding in enumerate(self.doc["observe"]):
                    if event.source == binding["source"] and event.kind == binding["kind"]:
                        self._plan(event, "observe", index, binding, memory, now)
            if result.transition:
                for index, binding in enumerate(self.doc["act"]):
                    if result.transition == binding["on"]:
                        self._plan(event, "act", index, binding, memory, now, result)
            pending = self.store.db.execute("SELECT COUNT(*) FROM operations WHERE mission=? AND status IN ('pending','running')", (self.key,)).fetchone()[0]
            if pending > self.doc["policy"]["max_pending"]:
                raise ContractError("backpressure")
            return {"status": "admitted", "event_id": event.id, "transition": result.transition,
                    "summary": result.summary, "state": result.state, "evidence": list(result.evidence)}

    async def _execute(self, operation: dict):
        now = self.clock()
        row = self.store.claim(operation["id"], now, self.doc["policy"]["max_concurrency"])
        if row is None:
            return False
        denial = self._denial(row["tool"])
        if denial:
            self.store.finish(row["id"], "denied", {"reason": denial}, None, now)
            return True
        tool = self.registry.tools[row["tool"]]
        source = self.store.event(self.key, row["event_id"])
        context = ToolContext(row["id"], self.key, Event.from_dict(json.loads(source["body"])),
                              self.store.pinned_memory(self.key, json.loads(row["memory_ids"])), row["deadline"])
        try:
            args = json.loads(row["arguments"])
            validate(tool.spec.input_schema, args)
            async with asyncio.timeout(max(0.001, (row["deadline"]-now)/1000)):
                result = await tool.execute(context, args)
            result.validate(tool.spec)
        except asyncio.CancelledError:
            self.store.finish(row["id"], "unknown", {"reason": "execution_cancelled"}, None, self.clock())
            raise
        except Exception:
            # A provider may have accepted the effect before an exception or invalid response.
            result = ToolResult("unknown", {"reason": "tool_failed_or_timed_out"})
        self.store.finish(row["id"], result.status, result.data, result.receipt, self.clock())
        return True

    def _apply_observations(self):
        rows = self.store.db.execute("""SELECT * FROM operations WHERE mission=? AND emits IS NOT NULL
            AND status NOT IN ('pending','running') AND applied=0 ORDER BY event_revision,id""", (self.key,)).fetchall()
        for row in rows:
            parent = self.store.event(self.key, row["event_id"])
            source = Event.from_dict(json.loads(parent["body"]))
            earlier = self.store.db.execute("""SELECT 1 FROM operations o JOIN events e
                ON e.mission=o.mission AND e.id=o.event_id WHERE o.mission=? AND o.role='observe'
                AND o.emits=? AND e.source=? AND o.applied=0 AND o.event_revision<? LIMIT 1""",
                (self.key, row["emits"], source.source, row["event_revision"])).fetchone()
            if earlier:
                continue
            successful = row["status"] == "succeeded" and source.quality == "known"
            tool = self.registry.tools[row["tool"]]
            available = row["completed"] if row["completed"] is not None else row["created"]
            event_id = "tool:"+row["id"]+(f':{row["outcome_revision"]}' if row["outcome_revision"] > 1 else "")
            if row["role"] == "observe":
                event = Event(event_id, source.source, row["emits"], source.observed_at_ms,
                              json.loads(row["result"] or "{}") if successful else {"reason": "observation_unavailable"},
                              "known" if successful else "unknown",
                              "synthetic" if tool.spec.configuration.get("synthetic") else "model", (source.id,))
            else:
                event = Event(event_id, source.source, row["emits"], available,
                              {"operation_id": row["id"], "tool": row["tool"], "status": row["status"],
                               "result": json.loads(row["result"] or "{}"), "receipt": row["receipt"]},
                              "known", "sensor", (source.id,))
            try:
                self.ingest(event, _derived=True, _available=available, _depth=parent["depth"]+1)
            except ContractError as error:
                if str(error) == "backpressure":
                    continue
                raise
            self.store.db.execute("UPDATE operations SET applied=1 WHERE id=? AND outcome_revision=? AND status=?",
                                  (row["id"], row["outcome_revision"], row["status"]))

    async def drain(self):
        """Dispatch a bounded batch; ingestion can continue while asynchronous tools wait."""
        async with self._dispatch_lock:
            self._check_definition()
            self.store.expire(self.key, self.clock())
            self._apply_observations()
            rows = self.store.db.execute("""SELECT * FROM operations WHERE mission=? AND status='pending'
                ORDER BY event_revision,id LIMIT ?""",
                (self.key, self.doc["policy"]["max_concurrency"])).fetchall()
            claimed = await asyncio.gather(*(self._execute(dict(row)) for row in rows))
            self._apply_observations()
            return sum(claimed)

    async def flush(self):
        while await self.drain():
            pass

    def cancel(self, operation_id: str):
        with self.store.transaction():
            row = self.store.db.execute("SELECT status FROM operations WHERE mission=? AND id=?", (self.key, operation_id)).fetchone()
            if row is None or row[0] != "pending":
                raise ContractError("only_pending_operations_can_be_cancelled")
            now = self.clock()
            self.store.db.execute("UPDATE operations SET status='cancelled',completed=? WHERE id=?", (now, operation_id))
            self.store.db.execute("INSERT INTO operation_history VALUES(NULL,?,?,?,?)", (operation_id, "cancelled", now, "{}"))
