# Changelog

## 2.0.0 (2026-11-27)

Version 2.0.0 of the package `SkillSharp.Client` replaces the old SkillSharp client 1.x. Namespace and assembly are
`OpenSmi.Client`. The client ports the Python `opensmi-client` at `9026539` (opensmi-core 0.7.1) and is tested
against opensmi-server 0.3.1. The old code is removed from the repository; its last state is commit `e57b3c0`, to be
tagged `skillsharp-legacy-final`.

There is no API compatibility with 1.x. Start with the quick start in the README and the examples in
`examples/OpenSmi.Client.Examples`.

### Breaking changes since the first port (`e8740c2`)

- Paths are `ObjectPath` instead of `string` and contain the OpenSMI folders: `/M1/Components/Gripper/SkillSet/Pick`
  instead of `/M1/Gripper/Pick`. Segments are unique (sanitized, duplicates suffixed `_2`, `_3`, ... in node ID
  order), and all collections are keyed by segment.
- Browse rules match `ObjectPath`. `Match.Path` and `Match.PathPrefix` check a string path when the rule is made.
- Users belong to the server: `RemoteServer.Users`, path `/Users/<name>`. `RemoteMachine.Users` is gone.
- Checks are `RemoteSkillCheck` below their skill (`/M1/SkillSet/Pick/FeasibilityCheck`).
- Variables cache a `Sample`; `ValueChanged` carries it and fires on value or status changes only. `Timestamp` is
  the source timestamp. `ReadHistoryAsync` returns samples; `TimestampedValue` and `ReadRawValueAsync` are gone.
- The component state is `MachineryItemState?` instead of a string. Skills have `CurrentStateSample`.
- DTOs (`ToDto()`, the `dto` example) are removed, as upstream did.
- Unknown states throw `OpenSmiUnknownStateException`.
- `RemoteComponent.CreateAsync` without parent gives the path `/Components/<name>`.

### New

- `Resolve`, `ResolveOptional` and `Find` (glob) on the server and, relative, on every object; `Match.Glob`.
- A variable listed in several groups is one object: the owner (`HasComponent`) gives the path, other groups
  (`Organizes`) are `Aliases`.
- Notifications are routed by monitored item and read `ErrorCode` from the MachineSet `AlertType`.
- Skill and component states are read while browsing, so they are known when `ConnectAsync` returns.
- `DependsOn` is filled on opensmi-server 0.3, which moved `Requirements` to the skill object.
- CI runs on Node.js 24 actions and uploads the NuGet package as artifact `nuget-package`.
- Tests against every example of opensmi-server and a port of the Python client's test server, each compared with the
  Python client as oracle.
