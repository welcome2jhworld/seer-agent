from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

from .builtins import builtin_registry, demo_registry
from .contracts import ContractError, Mission, encoded
from .host import Host, PROTOCOL, serve, writer
from .runtime import Runtime, wall_time_ms
from .sources import jsonl
from .store import Store


def output(value):
    print(encoded(value), flush=True)


def parser():
    root = argparse.ArgumentParser(description="Observe, remember and act through a versioned perception mission.")
    commands = root.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Run a JSONL input stream through a mission")
    run.add_argument("--mission", required=True, type=Path)
    run.add_argument("--db", required=True, type=Path)
    run.add_argument("--input", type=Path)
    run.add_argument("--replay", action="store_true", help="Use synthetic event time as the clock")
    run.add_argument("--demo", action="store_true", help="Register synthetic fixture model tools")
    run.add_argument("--allow-external", action="store_true", help="Permit mission-authorized external actions")
    run.add_argument("--plugin", action="append", default=[], help="Explicit installed seer_agent.plugins entry point")
    host = commands.add_parser("serve", help="Run missions for a host app over stdio JSON Lines (host protocol v1)")
    host.add_argument("--db", required=True, type=Path)
    host.add_argument("--allow-external", action="store_true", help="Permit mission-authorized external actions")
    host.add_argument("--plugin", action="append", default=[], help="Explicit installed seer_agent.plugins entry point")
    tools = commands.add_parser("tools", help="List explicitly registered tool contracts")
    tools.add_argument("--plugin", action="append", default=[])
    tools.add_argument("--demo", action="store_true")
    for command in ("inspect", "memory", "actions"):
        inspect = commands.add_parser(command)
        inspect.add_argument("--db", required=True, type=Path)
        inspect.add_argument("--mission", required=True, help="Mission identity and version, for example gate:1")
        inspect.add_argument("--limit", type=int, default=50)
    return root


async def run(args):
    if args.replay and args.allow_external:
        raise ContractError("replay_cannot_send_external_actions")
    if args.db.resolve() in {args.mission.resolve(), args.input.resolve() if args.input else None}:
        raise ContractError("database_must_be_separate_from_inputs")
    raw = args.mission.read_bytes()
    if len(raw) > 262144:
        raise ContractError("mission_too_large")
    mission = Mission.from_dict(json.loads(raw))
    registry = demo_registry() if args.demo else builtin_registry()
    for name in args.plugin:
        registry.load_plugin(name)
    args.db.parent.mkdir(parents=True, exist_ok=True)
    store = Store(args.db)
    replay_time = [0]
    stream = args.input.open(encoding="utf-8") if args.input else sys.stdin
    try:
        runtime = Runtime(mission, registry, store, allow_external=args.allow_external,
                          clock=(lambda: replay_time[0]) if args.replay else wall_time_ms)
        replay_time[0] = store.clock(mission.key)
        revision = store.db.execute("SELECT COALESCE(MAX(revision),0) FROM decisions WHERE mission=?", (mission.key,)).fetchone()[0]
        await runtime.flush()
        for event in jsonl(stream):
            if args.replay:
                if event.provenance != "synthetic":
                    raise ContractError("replay_requires_synthetic_input")
                replay_time[0] = max(replay_time[0], event.observed_at_ms)
            output(runtime.ingest(event))
            await runtime.flush()
            decisions = store.db.execute("SELECT * FROM decisions WHERE mission=? AND revision>? ORDER BY revision", (mission.key, revision)).fetchall()
            for decision in decisions:
                revision = decision["revision"]
                if decision["transition"]:
                    output({"type": "transition", "event_id": decision["event_id"], "transition": decision["transition"],
                            "summary": decision["summary"], "evidence": json.loads(decision["evidence"])})
        operations = store.operations(mission.key)
        output({"mission": mission.key, "state": runtime.state,
                "operations": [{k: row[k] for k in ("id", "tool", "role", "status", "receipt")} for row in operations]})
        return 3 if any(row["status"] in {"failed", "unknown", "denied", "running", "pending"} for row in operations) else 0
    finally:
        if args.input:
            stream.close()
        store.close()


def exclusive(path: Path):
    """One serve process owns a store, so it may mark work left running by a crash as unknown."""
    handle = open(path.with_name(path.name+".lock"), "a+b")
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise ContractError("store_in_use") from None
    return handle


async def host(args, write):
    registry = builtin_registry()
    for name in args.plugin:
        registry.load_plugin(name)
    args.db.parent.mkdir(parents=True, exist_ok=True)
    lock = exclusive(args.db)
    store = Store(args.db)
    try:
        return await serve(Host(registry, store, write, allow_external=args.allow_external), sys.stdin.buffer)
    finally:
        store.close()
        lock.close()


def main(argv=None):
    args = parser().parse_args(argv)
    if args.command == "serve":
        write = writer(sys.stdout.buffer)
        try:
            return asyncio.run(host(args, write))
        except (ContractError, ValueError, OSError) as error:
            # stdout carries only protocol messages; never print arbitrary details.
            write({"v": PROTOCOL, "type": "error", "id": "h0", "body": {
                "code": str(error) if isinstance(error, ContractError) else "invalid_input_or_storage"}})
            return 2
    try:
        if args.command == "tools":
            registry = demo_registry() if args.demo else builtin_registry()
            for name in args.plugin:
                registry.load_plugin(name)
            output([{"name": tool.spec.name, "version": tool.spec.version, "effect": tool.spec.effect,
                     "input_schema": tool.spec.input_schema, "output_schema": tool.spec.output_schema}
                    for tool in registry.tools.values()])
            return 0
        if args.command == "run":
            return asyncio.run(run(args))
        if not args.db.is_file() or not 1 <= args.limit <= 1000:
            raise ContractError("invalid_database_or_limit")
        store = Store(args.db)
        try:
            if not store.db.execute("SELECT 1 FROM missions WHERE id=?", (args.mission,)).fetchone():
                raise ContractError("mission_not_found")
            if args.command == "inspect":
                output({"mission": args.mission, "state": store.state(args.mission), "decisions": store.history(args.mission)[-args.limit:]})
            elif args.command == "actions":
                output(store.operations(args.mission)[-args.limit:])
            else:
                rows = store.db.execute("SELECT revision,available,admitted,body FROM events WHERE mission=? ORDER BY revision DESC LIMIT ?",
                                        (args.mission, args.limit)).fetchall()
                output([{**dict(row), "body": json.loads(row["body"])} for row in reversed(rows)])
        finally:
            store.close()
        return 0
    except (ContractError, ValueError, OSError) as error:
        # Exceptions from plugins/providers may contain credentials. Never print arbitrary details.
        output({"error": str(error) if isinstance(error, ContractError) else "invalid_input_or_storage"})
        return 2


if __name__ == "__main__":
    sys.exit(main())
