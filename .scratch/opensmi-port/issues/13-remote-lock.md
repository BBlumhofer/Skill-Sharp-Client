# 13 RemoteLock

Type: task
Status: resolved
Blocked by: 09

## Question

Die 8 Knoten werden exakt per BrowseName aufgelöst; ein fehlender Knoten führt zu
`OpenSmiProtocolException`. `InitLockAsync`/`ExitLockAsync`/`BreakLockAsync`/`RenewLockAsync`
liefern ein `LockResult`. Dazu kommen Cache und Signals (`LockedChanged`, `LockingUserChanged`,
`RenewDue`), `IsLockedByUs` (nur über den Usernamen) und `ReadStateAsync` mit einem einzigen Read.
`MaxInactiveLockTime` stammt aus DI i=6387, Fallback 60 000. `RemoteMachine.Lock` wirft ohne
Lock-Feature `InvalidOperationException`. Orakel: `remote_lock.py`.

## Akzeptanz (Integration)

- orchestrator Init → `IsLockedByUs`; Exit → `IsLocked == false`
- Exit ohne Lock → `LockResult.NotLocked`
- zweiter Client operator (höhere Priorität) ruft `BreakLockAsync` → bei Client 1 feuert `LockingUserChanged`

## Answer

`RemoteLock.cs`. Serververhalten: `BreakLock` übernimmt den Lock, auf einem freien Lock liefert es `NotLocked` (-1) und sperrt trotzdem. `ExitLock` ohne Lock gibt `BadUserAccessDenied` statt -1. Tests: `LockTests`.
