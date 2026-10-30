# 14 AutoLock

Type: task
Status: resolved
Blocked by: 13

## Question

`AutoLock(lock, breakLock, interval)` mit `LockAsync(blocking)`, `UnlockAsync`, `IsActive` und
`DisposeAsync`. Bei fremdem LockingUser wird neu gesperrt, bei `RenewDue` erneuert (höchstens ein
Renew-Task gleichzeitig). Orakel: `auto_lock.py`.

## Akzeptanz (Integration)

- orchestrator mit AutoLock; operator bricht den Lock und gibt ihn frei → orchestrator hält den Lock danach wieder

## Answer

`AutoLock` in `RemoteLock.cs`. Test `Auto_lock_relocks_after_another_user_released_it`.
