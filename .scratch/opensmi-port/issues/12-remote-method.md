# 12 RemoteMethod.CallAsync and dependencies

Type: task
Status: resolved
Blocked by: 11

## Question

`Call` is resolved at init. `CallAsync` calls it without arguments and returns
`FinalResultData.ReadAllAsync()`. `MinAccessLevel` as well as `DependsOn`/`DependencyOf` via
`Requires` live on `RemoteCallable`. Oracle: `remote_method.py`, `remote_callable.py`.

## Acceptance (integration)

- calling `DummyMethod` with a lock → result dictionary not empty
- `CompositeDummySkill.DependsOn` contains `ns=1;s=DummyMachine.SkillSet.DummySkill.SkillExecution`

## Answer

`RemoteMethod`, `MinAccessLevel`, `DependsOn`/`DependencyOf` (only `SkillExecution/Requirements`, see 01). Tests in `SkillTests`.
