# Runs an opensmi-server example (examples/<name>/main.py) unchanged, only on another endpoint port.
#
# Usage: python run_example.py <port> <path to main.py>
#
# The examples create their server with the default ServerConfiguration (port 4841); the runner replaces that class
# in opensmi.server._server by a subclass with the endpoint opc.tcp://127.0.0.1:<port>/server.

import os
import runpy
import sys

import opensmi.server._server as server_module

port, main = int(sys.argv[1]), os.path.abspath(sys.argv[2])
_Configuration = server_module.ServerConfiguration


class _PortConfiguration(_Configuration):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.ua_server.endpoint_address = f"opc.tcp://127.0.0.1:{port}/server"


server_module.ServerConfiguration = _PortConfiguration
os.chdir(os.path.dirname(main))
sys.path.insert(0, os.path.dirname(main))
sys.argv = [main]
runpy.run_path(main, run_name="__main__")
