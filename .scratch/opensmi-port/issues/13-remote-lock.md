# 13 RemoteLock

Type: task
Status: resolved
Blocked by: 09

## Question

The 8 nodes are resolved exactly by BrowseName; a missing node leads to
`OpenSmiProtocolException`. `InitLockAsync`/`ExitLockAsync`/`BreakLockAsync`/`RenewLockAsync`
return a `LockResult`. On top of that come a cache and signals (`LockedChanged`, `LockingUserChanged`,
`RenewDue`), `IsLockedByUs` (by username only) and `ReadStateAsync` with a single read.
`MaxInactiveLockTime` comes from DI i=6387, fallback 60 000. `RemoteMachine.Lock` throws
`InvalidOperationException` without the lock feature. Oracle: `remote_lock.py`.

## Acceptance (integration)

- orchestrator Init → `IsLockedByUs`; Exit → `IsLocked == false`
- Exit without a lock → `LockResult.NotLocked`
- a second client, operator (higher priority), calls `BreakLockAsync` → `LockingUserChanged` fires on client 1

## Answer

`RemoteLock.cs`. Server behavior: `BreakLock` takes over the lock; on a free lock it returns `NotLocked` (-1) and locks anyway. `ExitLock` without a lock returns `BadUserAccessDenied` instead of -1. Tests: `LockTests`.
