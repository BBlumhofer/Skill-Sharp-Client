# 15 RemoteUser

Type: task
Status: resolved
Blocked by: 09

## Question

`RemoteMachine.Users` from `MachineryBuildingBlocks/Users` with `Data` (AllowMultiple, UserLevel,
MaxAccessLevel, IsPresent) and `UserPresenceChanged`. In the DTO the field is called `LockPriority`. Oracle:
`remote_user.py`.

## Acceptance (integration)

- `Users` contains `orchestrator` with `UserLevel == "10"`

## Answer

`RemoteUser.cs`. Test `Users_expose_lock_priority`.
