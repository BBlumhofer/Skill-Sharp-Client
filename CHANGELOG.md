# Changelog

## 2.0.0 (2026-11-27)

`OpenSmi.Client` 2.0.0 replaces `SkillSharp.Client` 1.x completely. It is a port of the Python
`opensmi-client` at `9026539` (opensmi-core 0.7.1) and is tested against `opensmi-server` 0.3.1. The old
client is removed from the repository; its last state is commit `e57b3c0`.

### Breaking changes compared to the first port (`e8740c2`)

- **Paths** are `ObjectPath` instead of `string` and contain the OpenSMI folders:
  `/M1/Components/Gripper/SkillSet/Pick` instead of `/M1/Gripper/Pick`. Segments are unique: names with
  `/` or empty names are sanitized, duplicates get `_2`, `_3`, ... in NodeId order. All collections are
  keyed by segment.
- **Browse rules** match `ObjectPath`; `Match.Path`/`Match.PathPrefix` check their argument when the rule
  is made. New: `Match.Glob("/M1/**/SkillSet/*")`.
- **Users** belong to the server: `RemoteServer.Users`, path `/Users/<name>`, read from `Objects/Users`
  (fallback for servers before 0.3: `MachineryBuildingBlocks/Users` of every machine).
  `RemoteMachine.Users` is gone.
- **Checks** are `RemoteSkillCheck` with `Skill` and a path below their skill
  (`/M1/SkillSet/Pick/FeasibilityCheck`); `FeasibilityCheckSet`/`PreconditionCheckSet` hold them.
- **Samples**: `RemoteVariable.Sample` (value, variant, status, quality, timestamps). `ValueChanged`
  carries the sample and fires on value or status changes, not on a new timestamp alone. `Timestamp` is
  the source timestamp (fallback server timestamp). `ReadHistoryAsync` returns samples, `TimestampedValue`
  is gone. `WatchValueAsync` yields samples.
- **Component state** is `MachineryItemState?` instead of a string (`null` without a CurrentState node);
  `StatusChanged` carries a `Sample<MachineryItemState?>`. Skills have `CurrentStateSample`, and
  `StateChanged` carries a `Sample<SkillState>`.
- **DTOs** (`ToDto()`, `Dto.cs`, the `dto` example) are removed, as upstream did.
- Unknown states throw `OpenSmiUnknownStateException` (a `OpenSmiProtocolException`).
- `RemoteComponent.CreateAsync` without parent gets the path `/Components/<name>`.

### New

- `RemoteServer.Resolve`/`ResolveOptional`/`Find` and the same, relative, on every object.
- Variables listed in several groups are one object; the owner (HasComponent) gives the path, other
  places (Organizes) are `Aliases`.
- Notifications are routed by monitored item and read `ErrorCode` from the MachineSet AlertType, so
  alerts of opensmi-server 0.3 arrive with their code even when their source is a child object.
- Skill and component states are read while browsing, so they are known when `ConnectAsync` returns.
- Tests against every opensmi-server example (check, dictionary_entries, quick_start, rsl, dummy) and a
  port of the upstream test server, each compared with the Python client as oracle.

### Migration from SkillSharp.Client 1.x

There is no API compatibility with 1.x (no consumer left, see `docs/opensmi/SPEC.md` §1). The namespace is
`OpenSmi.Client`; start with the README's quick start and the examples in `examples/OpenSmi.Client.Examples`.
