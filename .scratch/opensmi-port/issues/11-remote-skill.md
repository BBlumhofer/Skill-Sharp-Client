# 11 RemoteSkill: Zustände, Start/Reset/Halt/Suspend, Waits

Type: task
Status: resolved
Blocked by: 09, 13

## Question

NodeId ist `SkillExecution`, der Name kommt vom Skill-Objekt, `IsFinite` aus der TypeDefinition des
Elternknotens. Die Methoden-NodeIds unter `StateMachine` werden einmal aufgelöst; der Call läuft
mit `ObjectId = StateMachine` und ohne Argumente. Dazu gehören Halt-Zähler, gecachter Zustand per
Subscription, `SkillHaltedException`, `StartWait.Completion` (nur finite Skills) und ein `timeout`,
der zu `TimeoutException` führt. Leerer Zustandstext → Halted, unbekannter Text →
`OpenSmiProtocolException`. `BadInvalidState` → `OpenSmiInvalidStateException`,
`BadRequiresLock`/`BadUserAccessDenied` → `OpenSmiAccessDeniedException`. Orakel: `remote_skill.py`.

## Akzeptanz

- Unit: Parsing von `""`, `"Running"` und `"Bogus"`
- Integration: DummySkill Reset → Ready, Start(Completion) → Completed
- Integration: Start ohne Lock → `OpenSmiAccessDeniedException`
- Integration: Suspend auf DummySkill → `SkillNotSuspendableException`

## Answer

`RemoteSkill` in `RemoteCallable.cs`. Der Server prüft den FSM-Zustand **vor** dem Lock: Start ohne Lock im falschen Zustand liefert `BadInvalidState`. Einmal trat beobachtet `Completing` nach einem bereits gemeldeten `Completed` auf; das ließ sich nicht reproduzieren. Falls es wiederkommt, das Server-Timing prüfen. Tests: `SkillTests`, `SkillStateParsingTests`.
