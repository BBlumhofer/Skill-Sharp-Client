# 11 RemoteSkill: states, Start/Reset/Halt/Suspend, waits

Type: task
Status: resolved
Blocked by: 09, 13

## Question

The NodeId is `SkillExecution`, the name comes from the skill object, `IsFinite` from the TypeDefinition of the
parent node. The method NodeIds under `StateMachine` are resolved once; the call runs
with `ObjectId = StateMachine` and without arguments. This includes a halt counter, a cached state via
subscription, `SkillHaltedException`, `StartWait.Completion` (finite skills only) and a `timeout`
that leads to `TimeoutException`. Empty state text → Halted, unknown text →
`OpenSmiProtocolException`. `BadInvalidState` → `OpenSmiInvalidStateException`,
`BadRequiresLock`/`BadUserAccessDenied` → `OpenSmiAccessDeniedException`. Oracle: `remote_skill.py`.

## Acceptance

- Unit: parsing of `""`, `"Running"` and `"Bogus"`
- Integration: DummySkill Reset → Ready, Start(Completion) → Completed
- Integration: Start without a lock → `OpenSmiAccessDeniedException`
- Integration: Suspend on DummySkill → `SkillNotSuspendableException`

## Answer

`RemoteSkill` in `RemoteCallable.cs`. The server checks the FSM state before the lock: Start without a lock in the wrong state returns `BadInvalidState`. Once, `Completing` was observed after an already reported `Completed`; this could not be reproduced. If it comes back, check the server timing. Tests: `SkillTests`, `SkillStateParsingTests`.
