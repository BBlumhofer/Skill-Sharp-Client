# Skills and methods

The state machine and the meaning of each skill state are described in the
[OpenSMI Server documentation](https://github.com/SmartFactory-KL/opensmi-server/blob/main/docs/skills.md). This page
covers how the client drives skills.

## Running a skill

A skill needs the lock of its machine (see [locking](locking.md)) and starts from `Ready`, so reset it first.

```csharp
var skill = machine.SkillSet["DummySkill"];
if (await skill.ReadCurrentStateAsync() != SkillState.Ready)
    await skill.ResetAsync(wait: true);

await skill.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["X"] = 5, ["Y"] = 42 });
await skill.StartAsync(StartWait.Completion, timeout: TimeSpan.FromSeconds(30));
var results = await skill.FinalResultData.ReadAllAsync();
await skill.ResetAsync(wait: true);   // reset after reading the results
```

`StartAsync` calls `Start` and, depending on `wait`, returns at once (`StartWait.None`), once the skill is `Running`
or `Completed` (`StartWait.RunningOrCompleted`), or once it is `Completed` (`StartWait.Completion`). `Completion`
throws `InvalidOperationException` for continuous skills, which never complete; `IsFinite` tells them apart.

`ResetAsync`, `HaltAsync` and `SuspendAsync` wait for `Ready`, `Halted` and `Suspended` when `wait` is `true`.
`SuspendAsync` throws `SkillNotSuspendableException` if the skill has no `Suspend` method (`IsSuspendable`).
`WaitForStateAsync(state)` waits without calling anything.

Every wait accepts a `timeout` (then `TimeoutException`) and a `CancellationToken`. If the skill halts while you wait
for another state, the wait throws `SkillHaltedException`. Calling a skill in the wrong state throws
`OpenSmiInvalidStateException`, calling it without the lock `OpenSmiAccessDeniedException`.

## State

`CurrentState` is the cached state, updated by the subscription and read once while browsing. Without a value, for
example with a bad status, it is `Halted`. `CurrentStateSample` carries the status and timestamps of the state (see
[variables](variables.md#samples)), and `ReadCurrentStateAsync` reads it from the server. An unknown state text
throws `OpenSmiUnknownStateException`; in the subscription it is logged and ignored.

`StateChanged` and `WatchStateAsync` report changes (see [events](events.md)). Do not wait for a later state inside
a `StateChanged` handler, it would block the notification it waits for.

## Feasibility and precondition checks

A check is a `RemoteSkillCheck`, a skill-like object below its skill (`/M1/SkillSet/Pick/FeasibilityCheck`).
`FeasibilityCheckSet` and `PreconditionCheckSet` of a component are keyed by the skill's segment, and `Skill` leads
back to the skill.

```csharp
var check = machine.FeasibilityCheckSet["FullSkill"];
await check.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["X"] = 2, ["Y"] = 3 });
await check.StartAsync(StartWait.Completion);
var id = check.FinalResultData["FeasibilityID"].Value;

await check.Skill.ParameterSet["FeasibilityID"].WriteValueAsync(id);
await check.Skill.StartAsync(StartWait.Completion);
```

## Methods

A method has no state machine. `CallAsync` calls it without input arguments and returns its `FinalResultData`, so
write `ParameterSet` first.

```csharp
var method = machine.MethodSet["DummyMethod"];
await method.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["A"] = 5, ["B"] = 42 });
var results = await method.CallAsync();
```

## Requirements

Skills and methods expose `MinAccessLevel` (`null` if the server does not provide it) and their dependencies:
`DependsOn` lists the node IDs their `Requirements` folder references with `Requires`, `DependencyOf` the reverse.
For skills the client reads `Requirements` below the skill object (opensmi-server 0.3) and below `SkillExecution`
(older servers).
