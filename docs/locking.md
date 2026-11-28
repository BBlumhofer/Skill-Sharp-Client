# Locking

Writing parameters and calling skills or methods needs the lock of the machine (or of a component that has its own
lock). The lock follows the DI `LockingServicesType`.

```csharp
var result = await machine.Lock.InitLockAsync();   // LockResult.Ok or NotLocked
// ... use the machine ...
await machine.Lock.ExitLockAsync();
```

`InitLockAsync`, `ExitLockAsync`, `BreakLockAsync` and `RenewLockAsync` return the server's `LockResult`; a bad
status code throws, e.g. `BadLocked` if someone else holds the lock. `BreakLockAsync` takes the lock from a user with
lower priority (`RemoteUser.LockPriority`).

The cached members `IsLocked`, `LockingUser`, `LockingClient`, `LockedSince`, `RemainingLockTimeMs` and
`IsLockedByUs` follow the subscription. `ReadStateAsync` reads all four lock variables in one request. The client owns
the lock when `LockingUser` is its user name.

## AutoLock

`AutoLock` takes the lock, renews it before it expires and takes it again when another user releases it.

```csharp
await using var autoLock = new AutoLock(machine.Lock);
await autoLock.LockAsync();   // returns once the lock is ours; blocking: false locks in the background
```

`new AutoLock(lock, breakLock: true)` breaks the lock instead of waiting for it. Renewing starts when the remaining
time drops below `RenewThreshold` (default half of `MaxInactiveLockTimeMs`, which is read from the server).
Disposing the `AutoLock` releases the lock.

## Reconnecting with the same user

opensmi-server notices a closed session only after up to a second and then releases the lock of that session's user,
even if a new session of the same user has taken the lock in the meantime
([issue #5](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/5)). After reconnecting with the same user, wait
a moment before taking the lock, or use `AutoLock`, which takes it again.
