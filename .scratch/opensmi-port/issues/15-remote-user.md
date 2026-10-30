# 15 RemoteUser

Type: task
Status: resolved
Blocked by: 09

## Question

`RemoteMachine.Users` aus `MachineryBuildingBlocks/Users` mit `Data` (AllowMultiple, UserLevel,
MaxAccessLevel, IsPresent) und `UserPresenceChanged`. Im DTO heißt das Feld `LockPriority`. Orakel:
`remote_user.py`.

## Akzeptanz (Integration)

- `Users` enthält `orchestrator` mit `UserLevel == "10"`

## Answer

`RemoteUser.cs`. Test `Users_expose_lock_priority`.
