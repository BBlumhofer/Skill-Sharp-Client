"""Oracle: every object the Python opensmi-client (9026539) finds below "/", as "<path> <type> [<aliases>]".

Usage: python dump_paths.py <url> <user> <output file>
  dummy-objects.txt:        opensmi-server 0.3.1 examples/dummy (opc.tcp://localhost:4843/server, orchestrator)
  paths-server-objects.txt: ../servers/paths_server.py (opc.tcp://127.0.0.1:<port>/server, developer)
"""
import asyncio
import pathlib
import sys

from opensmi.client import RemoteServer


async def main(url: str, user: str, output: str) -> None:
    async with RemoteServer(url, username=user, password=user, auto_reconnect=False) as server:
        lines = [
            " ".join([str(obj.path), type(obj).__name__, *([",".join(sorted(map(str, obj.aliases)))] if obj.aliases else [])])
            for obj in server.find("/**")
        ]
    pathlib.Path(output).write_text("\n".join(lines) + "\n", encoding="utf8")


asyncio.run(main(*sys.argv[1:4]))
