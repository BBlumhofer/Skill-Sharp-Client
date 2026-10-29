"""Oracle: path and node id of every object in DummyMachine.dto() of the Python opensmi-client (e8740c2).

Run with PYTHONPATH=<opensmi-client checkout>/src against the dummy server; writes dummy-machine-paths.txt.
"""
import asyncio, dataclasses, pathlib
from opensmi.client import RemoteServer

def walk(node, out):
    if isinstance(node, dict):
        if "path" in node and "ua_node_id" in node:
            out.add(f"{node['path']} {node['ua_node_id']}")
        for v in node.values():
            walk(v, out)
    elif isinstance(node, list):
        for v in node:
            walk(v, out)

async def main():
    async with RemoteServer("opc.tcp://localhost:4843/server", username="orchestrator", password="orchestrator") as s:
        out = set()
        walk(dataclasses.asdict(s.machines["DummyMachine"].dto()), out)
    pathlib.Path(__file__).with_name("dummy-machine-paths.txt").write_text("\n".join(sorted(out)) + "\n", encoding="utf8")

asyncio.run(main())
