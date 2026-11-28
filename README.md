![OpenSMI Logo](https://smartfactory.de/wp-content/uploads/2026/09/Logo_OpenSMI.png)

[![CI](https://github.com/BBlumhofer/Skill-Sharp-Client/actions/workflows/ci.yml/badge.svg)](https://github.com/BBlumhofer/Skill-Sharp-Client/actions/workflows/ci.yml)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%2010.0-blue)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/license-MIT-blue)](#license)

# OpenSMI Client for .NET

A C# library for connecting to SMI-compliant OPC UA servers, such as those built with
[OpenSMI Server](https://github.com/SmartFactory-KL/opensmi-server). It is a port of the Python
[OpenSMI Client](https://github.com/SmartFactory-KL/opensmi-client) (commit `9026539`) and behaves like it: it browses
the server at runtime and exposes machines, components, resources, skills, methods, variables, the lock, users and
notifications through one API, without a generated, server-specific client.

You can also skip browsing and build objects from node IDs you already know, or browse only the parts you need with
browse rules.

The package is `SkillSharp.Client`; version 2.0.0 replaces the old SkillSharp client 1.x. Namespace and assembly are
`OpenSmi.Client`.

## Requirements

- .NET 8 or .NET 10
- An SMI-compliant OPC UA server, e.g. one built with [OpenSMI Server](https://github.com/SmartFactory-KL/opensmi-server)

## Installation

The package is not on NuGet yet. Build it from source, or download the `nuget-package` artifact of a CI run:

```bash
dotnet pack src/OpenSmi.Client -c Release -o packages
```

```bash
dotnet add package SkillSharp.Client --version 2.0.0 --source ./packages
```

## Quick Start

```csharp
using OpenSmi.Client;

// remember: do not commit the actual password to version control...
await using var server = await new RemoteServer(new()
{
    Url = "opc.tcp://localhost:4841/server", Username = "operator", Password = "operator",
}).ConnectAsync();

var machine = server.Machines["ExampleMachine"];
await machine.Lock.InitLockAsync();

var skill = machine.SkillSet["ExampleSkill"];
if (await skill.ReadCurrentStateAsync() != SkillState.Ready)
    await skill.ResetAsync(wait: true);

await skill.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["X"] = 5, ["Y"] = 10 });
await skill.StartAsync(StartWait.Completion);
Console.WriteLine(string.Join(", ", await skill.FinalResultData.ReadAllAsync()));
await skill.ResetAsync(wait: true);
```

This connects to the `ExampleMachine` of the
[OpenSMI Server quick start](https://github.com/SmartFactory-KL/opensmi-server#quick-start), locks it, runs
`ExampleSkill` once and prints `ComputationResult=15`.

## Core Concepts

| Concept | What it is |
|---|---|
| [Connecting](docs/connection.md) | `RemoteServer`: options, retry, security, reconnect, status |
| [Browsing and paths](docs/browsing.md) | Machines, components and resources, browse rules, `ObjectPath`, `Resolve` and `Find` |
| [Skills and methods](docs/skills.md) | Starting, resetting and waiting for skills, feasibility and precondition checks, methods |
| [Variables](docs/variables.md) | Values as `Sample` with status and timestamps, reading, writing, history, units |
| [Events](docs/events.md) | Change events, `Watch…Async` streams and notifications from the server |
| [Locking](docs/locking.md) | `RemoteLock` and `AutoLock` |
| [Differences to Python](docs/deviations.md) | Where the C# client deliberately behaves differently |
| [Development](docs/development.md) | Tests against opensmi-server, oracle files, CI |

## Examples

[examples/OpenSmi.Client.Examples](examples/OpenSmi.Client.Examples) contains a C# version of every example of the
Python client. The first argument selects the example, the second one the server URL (optional):

```bash
dotnet run --project examples/OpenSmi.Client.Examples -- example opc.tcp://localhost:4843/server
```

| Example | What it does |
|---|---|
| `auto_lock` | keeps the machine's lock with `AutoLock` until Ctrl+C |
| `browse_rules` | browses only what the browse rules ask for |
| `direct_component` | connects without browsing and builds a component from its node ID |
| `example` | locks the machine, starts `StartupSkill` and runs `DummySkill` |
| `history` | reads the last 10 values of a historized variable |
| `method_call` | calls `DummyMethod` |
| `quick_start` | runs `ExampleSkill` of `opensmi-server/examples/quick_start` (`opc.tcp://localhost:4841/server`) |
| `security_policy` | connects with Basic256Sha256 and SignAndEncrypt |
| `subscription_component` | prints notifications and state changes for 120 s |
| `subscription_server` | prints server status and time until Ctrl+C |
| `subscription_variable` | prints value changes for 120 s |

All other examples expect the dummy machine of `opensmi-server/examples/dummy` (`python main.py`, endpoint
`opc.tcp://localhost:4843/server`) with the users `visitor`, `operator` and `orchestrator` (password = user name).

## Project Status

The client covers the Python client at `9026539` and is tested against opensmi-server 0.3.1, see
[CHANGELOG.md](CHANGELOG.md). Not yet ported: the fallback to the `Messages` node of old v4 servers and the
SmartFactory-specific parts (ports, storages, recovery), which will become a separate package. Neither the tests nor
the Python oracle cover real SmartFactory hardware; check the client yourself before using it on real machines.

## License

MIT, like OpenSMI. The license file is still missing (issue #3).

---

*Code, tests and documentation were written mostly with AI assistance (Claude, Anthropic, via Claude Code) and reviewed
by the maintainer. Commits with AI contributions carry a `Co-Authored-By: Claude` line.*
