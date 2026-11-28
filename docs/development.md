# Development

## Layout

| Path | Content |
|---|---|
| `src/OpenSmi.Client` | the library |
| `tests/OpenSmi.Client.Tests` | unit and integration tests in one xUnit project |
| `examples/OpenSmi.Client.Examples` | the examples of the Python client, `net10.0` only |
| `tools/generate_units.py` | generates `Unit.g.cs` from opensmi-core |
| `.scratch/opensmi-port` | decisions and tickets of the port |

## Tests

```bash
dotnet test OpenSmi.Client.slnx
```

The integration tests run against the real Python opensmi-server 0.3.1, not against mocks. They need Python 3.12 or
newer (as command `python`) with `opensmi-server` installed and its `examples` folder. Only pure functions (paths,
sample updates, parsing) have unit tests without a server.

| Variable | Meaning |
|---|---|
| `OPENSMI_SERVER_DIR` | path to `opensmi-server/examples/dummy`; default `../opensmi-server/examples/dummy` next to the repository. The other examples are expected next to it. |
| `OPENSMI_TEST_PORT` | port of the shared dummy server, default 4843. A server already listening there is reused. |

Some test classes start their own servers on fixed ports in temporary folders:

- `ServerExamplesTests` starts every example of opensmi-server (`check`, `dictionary_entries`, `quick_start`, `rsl`)
  unchanged through `servers/run_example.py` and runs what the example is for.
- `ObjectModelTests` uses `servers/paths_server.py`, a port of the test server of the Python client's tests, with every
  kind of object, unusual names, a variable with two owners and the user layout of servers before 0.3.

After each test against a server the tests wait 1.2 s (`ServerFixture.SettleAsync`), because of the lock behavior
described in [locking](locking.md#reconnecting-with-the-same-user).

## Oracle

For every server, `tests/OpenSmi.Client.Tests/oracle/` holds the objects the Python client finds (path, type, aliases),
and the tests compare `Find("/**")` with it. To regenerate a file, start the server and run:

```bash
python oracle/dump_paths.py opc.tcp://localhost:4843/server orchestrator oracle/dummy-objects.txt
```

## CI

`.github/workflows/ci.yml` runs the tests on Ubuntu with .NET 8 and 10 against opensmi-server `v0.3.1`, then packs the
library and uploads it as artifact `nuget-package`.
