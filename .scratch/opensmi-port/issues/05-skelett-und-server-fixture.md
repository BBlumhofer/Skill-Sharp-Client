# 05 Skeleton and server fixture

Type: task
Status: resolved
Blocked by: 02

## Question

New solution `OpenSmi.Client.slnx` with `src/OpenSmi.Client` and `tests/OpenSmi.Client.Tests`,
`Directory.Build.props` (Nullable, TreatWarningsAsErrors) and `.gitignore`. Move the old code to
`legacy/`. The xUnit fixture starts `python main.py` from
`opensmi-server/examples/dummy` (path via `OPENSMI_SERVER_DIR`), waits for port 4843 and stops
the process at the end.

Ponytail: no separate IntegrationTests project; unit and integration tests live in one
test project.

## Acceptance

- `dotnet build` without warnings
- one test connects to the started server

## Answer

Solution `OpenSmi.Client.slnx`, `Directory.Build.props` (net10.0, Nullable, TreatWarningsAsErrors), `.gitignore`. The old code now lives in `legacy/` (no `git mv`, the repo is broken, see 04). The fixture `tests/OpenSmi.Client.Tests/ServerFixture.cs` starts the dummy server only if port 4843 is free. Test packages are pinned to the local NuGet cache (Test.Sdk 17.14.1, xunit 2.9.3, runner 3.1.4) because no feed was reachable.
