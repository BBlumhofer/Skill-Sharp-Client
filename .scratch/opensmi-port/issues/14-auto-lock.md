# 14 AutoLock

Type: task
Status: resolved
Blocked by: 13

## Question

`AutoLock(lock, breakLock, interval)` with `LockAsync(blocking)`, `UnlockAsync`, `IsActive` and
`DisposeAsync`. If another LockingUser holds the lock, it locks again; on `RenewDue` it renews (at most one
renew task at a time). Oracle: `auto_lock.py`.

## Acceptance (integration)

- orchestrator with AutoLock; operator breaks the lock and releases it → orchestrator holds the lock again afterwards

## Answer

`AutoLock` in `RemoteLock.cs`. Test `Auto_lock_relocks_after_another_user_released_it`.
