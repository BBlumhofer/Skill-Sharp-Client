# 12 RemoteMethod.CallAsync und Abhängigkeiten

Type: task
Status: resolved
Blocked by: 11

## Question

`Call` wird beim Init aufgelöst. `CallAsync` ruft ihn ohne Argumente auf und gibt
`FinalResultData.ReadAllAsync()` zurück. `MinAccessLevel` sowie `DependsOn`/`DependencyOf` über
`Requires` liegen auf `RemoteCallable`. Orakel: `remote_method.py`, `remote_callable.py`.

## Akzeptanz (Integration)

- `DummyMethod` mit Lock aufrufen → Ergebnis-Dictionary nicht leer
- `CompositeDummySkill.DependsOn` enthält `ns=1;s=DummyMachine.SkillSet.DummySkill.SkillExecution`

## Answer

`RemoteMethod`, `MinAccessLevel`, `DependsOn`/`DependencyOf` (nur `SkillExecution/Requirements`, siehe 01). Tests in `SkillTests`.
