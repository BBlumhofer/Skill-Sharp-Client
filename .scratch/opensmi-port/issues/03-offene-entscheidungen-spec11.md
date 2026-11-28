# 03 Open decisions from SPEC §11

Type: grilling
Status: resolved
Blocked by: none

## Question

1. Package/repo: `OpenSmi.Client` in this repo or a new repo `opensmi-client-dotnet`?
2. Only `net10.0` or `net8.0` as well?
3. Is the SmartFactory package (P6) needed?
4. `AsyncSignal<T>` or `IObservable`/`IAsyncEnumerable`?
5. Generate a `Unit` enum or leave it out?

## Working assumption until resolved

Until these are resolved, the SPEC proposals apply, and all of them are easy to change:
`OpenSmi.Client` in this repo, only `net10.0`, no SF package, `AsyncSignal<T>`, no
`Unit` enum.

## Answer

Decided with the user (2026-10-29):
1. Stays in this repo; rename later.
2. `net8.0` in addition → [24 Multi-target net8.0/net10.0](24-multi-target-net8.md).
3. The SmartFactory package comes afterwards as a separate project; this map delivers only the client.
4. Not fixed: "reassess cleanly, no commitment to what exists" → [25 Notification model](25-benachrichtigungsmodell.md).
5. The `Unit` enum is generated → [26 Generate unit enum](26-unit-enum.md).
