# Map: Skill-Sharp-Client → OpenSMI client for C#

Label: wayfinder:map

## Destination

A C# library `OpenSmi.Client` that behaves towards the `opensmi-server` like the
Python client `opensmi-client @ e8740c2`. Acceptance: SPEC §12
([docs/opensmi/SPEC.md](../../docs/opensmi/SPEC.md)).

## Notes

- Execution belongs to this map (a departure from the wayfinder default): the user asked for
  the tickets to be implemented with TDD afterwards. Implementation tickets have `Type: task`
  (AFK) and are worked with the skills `tdd` and `ponytail`.
- The oracle is always the Python client. Reference checkout:
  `C:\Users\BenjaminBlumhofer\Repos\CA-Modul\opensmi-port\opensmi-client` (e8740c2). For questions
  about behavior, the code there comes first, then [OI](../../docs/opensmi/analyse/opensmi-client-inventar.md),
  and only then SPEC. Deviations exist only where SPEC §10 lists them.
- Test server: `cd ..\opensmi-server\examples\dummy; python main.py` →
  `opc.tcp://localhost:4843/server`. Users (= password, priority): visitor 0, planner 5,
  orchestrator 10, remote 15, operator 20, developer 100.
- Python oracle against the same server:
  `$env:PYTHONPATH="<reference checkout>\src"; python oracle.py`. The installed
  `OpenSMI-Client 1.0.0rc2` does not match the installed core and cannot be used.
- Seams (TDD):
  1. Public API of `OpenSmi.Client` against the real `opensmi-server`
     (integration tests). This is the main seam; the SDK is not mocked.
  2. Pure functions without a server: BrowseRules/Matcher, state parsing, notification parsing,
     `AsyncSignal`. Unit tests run here.
- Ponytail: anything the Python version lacks and SPEC does not require is not built.
- Parallel agents: each agent gets its own port (`OPENSMI_TEST_PORT`, its own
  `TestServer` ports) and its own build directory (`dotnet test OpenSmi.Client.slnx
  --artifacts-path .artifacts/<name>`). Agents do not touch files of another open ticket.

## Decisions so far

- [01 Location of `Requirements`](issues/01-requirements-lage.md): it sits on the skill and on
  `SkillExecution`. Like Python, only `SkillExecution` is read; the search in two places is dropped.
- [02 Test server can be started locally](issues/02-testserver-lokal.md): yes, Python 3.12 and
  opensmi-server are installed as editable, endpoint `/server` on port 4843.
- [04 Repo repair](issues/04-repo-reparatur-und-zugangsdaten.md): `.git` recreated from origin, branch
  `opensmi-port`; the passwords are only test accounts.
- [05 Skeleton](issues/05-skelett-und-server-fixture.md): one test project; the fixture reuses a
  running server. The old code lives in `legacy/`.
- [06 RemoteServer](issues/06-remote-server-connect.md): `DefaultSessionFactory` without reflection,
  NoSecurity only for now.
- [07 Signals/Subscriptions](issues/07-signals-und-subscriptions.md): serial channel dispatch;
  no unsubscribe, the SDK does the chunking.
- [08 BrowseRules](issues/08-browse-rules.md): 1:1 from `browse.py`.
- [09 Browse model](issues/09-browse-modell-und-variablen.md): the order of the dictionaries
  is not a contract; custom enum types are not ported.
- [10 Writing](issues/10-variablen-schreiben.md): batch write with a single-write fallback.
- [11 RemoteSkill](issues/11-remote-skill.md): the server checks the state before the lock.
- [12 RemoteMethod](issues/12-remote-method.md): as in Python.
- [13 RemoteLock](issues/13-remote-lock.md): `BreakLock` takes over the lock; `ExitLock` without a lock
  returns AccessDenied.
- [14 AutoLock](issues/14-auto-lock.md): as in Python.
- [15 RemoteUser](issues/15-remote-user.md): `LockPriority`.
- [16 Notifications](issues/16-notifications.md): without the fallback to `Messages`.
- [17 DTOs](issues/17-dto.md): parity with the Python oracle (113 objects) is tested.
- [18 History, Security, Reconnect](issues/18-history-security-reconnect.md): history is done,
  the rest is split into 21, 22 and 23.
- [19 Examples and docs](issues/19-beispiele-und-doku.md): all 12 Python examples in one
  console project (`example.py` → `ColdStartExample`), new README, `DEVIATIONS.md` with a test per row.
- [20 Own test server](issues/20-eigener-testserver.md): `TestServer` with its own port and
  optional encryption, stop/restart; the port of the collection server via `OPENSMI_TEST_PORT`.
- [03 Open decisions](issues/03-offene-entscheidungen-spec11.md): stays in this repo, net8.0 in
  addition, the SF package comes afterwards, `Unit` is generated; the signal model is reassessed.
- [21 Security](issues/21-security.md): `SecurityOptions` with folder stores, client certificate
  created once, exact endpoint selection; an untrusted server aborts without a retry.
- [22 Reconnect in the SDK](issues/22-reconnect-im-sdk.md): a recreate returns a new session with
  cloned subscriptions; `RemoteServer.Session` and `SubscriptionManager` have to be rebound,
  the objects stay. Backoff 1 s to 30 s.
- [23 Reconnect](issues/23-reconnect.md): KeepAlive → `Reconnecting`, the new session is adopted,
  `SubscriptionManager.Rebind`, objects stay identical; the heartbeat rebuilds data and event items.
  Leftovers in 28.
- [27 Best practice for notifications](issues/27-benachrichtigung-best-practice.md): network clients
  (MQTTnet 4, RabbitMQ 7, Azure IoT v2, SignalR, NATS) use async events plus `IAsyncEnumerable` for
  waiting; Rx is rarely used. Recommended: async events, watch streams and WaitFor.
- [24 Multi-target](issues/24-multi-target-net8.md): `net8.0;net10.0`, examples only net10.0,
  test targets run one after the other.
- [25 Notification model](issues/25-benachrichtigungsmodell.md): async events (one after the other),
  watch streams as `IAsyncEnumerable`, WaitFor stays; no Rx; unit enum in PascalCase. Implemented in 29.
- [28 Reconnect leftovers](issues/28-reconnect-reste.md): `WatchdogInterval` (1 s), without AutoReconnect
  → `Disconnected`, short network drop tested with a TcpProxy. Item retry skipped (cannot be tested honestly).
- [30 Custom data types](issues/30-custom-datentypen.md): enum DataTypes via `DataTypeDefinition`
  (without the ComplexTypes package); `Value` = member name, writing by name or number; test with its own test type.
- [31 Hardening disconnect](issues/31-disconnect-absichern.md): concurrent `DisconnectAsync` calls share
  one teardown; a call from the `Disconnecting` handler returns immediately.
- [32 CI](issues/32-ci.md): GitHub Actions on Linux, opensmi-server pinned to main 29e4d4a; has never run.
- [29 Notification model implemented](issues/29-benachrichtigungsmodell-umsetzen.md): `AsyncEvents.cs`
  (delegate, EventArgs, serial raise, watch buffer 8/DropOldest); `AsyncSignal` removed; units in PascalCase.
- [26 Unit enum](issues/26-unit-enum.md): 2133 units generated from opensmi-core
  (`tools/generate_units.py`), Python names (`CENTIMETRE`), helpers in `Units`, mapping via `eu.ToUnit()`.

- [34 Version 2.0.0](issues/34-version-2-upstream-9026539.md): upstream 9026539 / opensmi-server 0.3.1
  ported (paths, samples, users on the server, DTOs removed), tests against all server examples, `legacy/`
  deleted, tag `skillsharp-legacy-final`, package `SkillSharp.Client` 2.0.0, requirements read from the
  skill object, actions on Node 24.

## Not yet specified

- Comparison against the example servers of opensmi-server (2026-10-29): `quick_start` (19 objects) and
  `dictionary_entries` (15) are identical to the Python client. `rsl` does not start on the local feature
  branch 547ce5b because of a server bug; on opensmi-server `main` (29e4d4a) `rsl` runs, and
  Python and C# return the same 19 objects.
- Retry of individual failed monitored items (SPEC §7.1): so far no realistic trigger
  against the dummy server; it becomes a ticket only once a real server shows the problem.
- NuGet release of 2.0.0 and repo rename: waiting on whether SmartFactory-KL adopts the client (#9).

## Out of scope

- SmartFactory package (ports, storages, neighbors, recovery): a separate project after the client
  ([03](issues/03-offene-entscheidungen-spec11.md)).
- Server implementation, porting `opensmi-core` beyond what the client needs (SPEC §1).
- API compatibility with the old `SkillSharp.Client` (no consumer left, SPEC §1).

## Docs (2026-11-28)

SPEC, UMBAUPLAN and the analysis inventories under `docs/opensmi/` have been removed (in the Git history up to `33b317d`).
The docs now follow the layout of opensmi-server: README plus concept pages under `docs/`, in English;
`DEVIATIONS.md` is `docs/deviations.md`. References to SPEC in the tickets refer to that old state.

## Open items on GitHub (from 2026-10-30)

Whatever is still open after PR [#2](https://github.com/BBlumhofer/Skill-Sharp-Client/pull/2) is tracked
as a GitHub issue, no longer here:
[#3 LICENSE/NOTICE](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/3), [#4 empty catch](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/4), [#5 lock bug in opensmi-server](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/5),
[#6 tag and delete legacy/ (ticket 33)](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/6), [#7 item retry](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/7), [#8 SmartFactory package](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/8),
[#9 release/NuGet/rename](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/9), [#10 CI actions](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/10), [#11 test NodeId writing](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/11),
[#12 Completing after Completed](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/12).
