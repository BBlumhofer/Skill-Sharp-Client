# Browsing and paths

## What the client browses

`ConnectAsync` starts at the Machinery `Machines` folder (or a folder named `Machines` below `Objects`) and browses
every machine recursively: components and their components, resources, skills with their checks, methods, the lock,
and the variable groups `Identification`, `Attributes`, `Monitoring` and `ParameterSet` (skills and methods also have
`FinalResultData`). Users come from the server-wide `Objects/Users` folder; servers before opensmi-server 0.3 list
them below each machine's `MachineryBuildingBlocks/Users`, which the client reads instead.

```csharp
var machine = server.Machines["DummyMachine"];
var component = machine.Components["DummyComponent"];
var skill = component.SkillSet["DummySkillInComponent"];
var user = server.Users["operator"];
```

A child that fails to browse is left out; the reason is in `InitExceptions` of its parent. Connection loss, timeouts
and cancellation abort the browse instead.

## Paths

Every object has a path like in the Python client: the parent's path, the OpenSMI folder (`Components`, `Resources`,
`SkillSet`, `MethodSet`, `Users`) and the object's segment.

| Object | Path |
|---|---|
| machine | `/M1` |
| component | `/M1/Components/Gripper` |
| skill | `/M1/Components/Gripper/SkillSet/Grip` |
| feasibility check | `/M1/SkillSet/Pick/FeasibilityCheck` |
| method | `/M1/MethodSet/Home` |
| variable | `/M1/SkillSet/Pick/ParameterSet/Speed` |
| lock | `/M1/Lock` |
| user | `/Users/operator` |

The segment is the browse name. A `/` in it becomes `_`, an empty name becomes `_`, and siblings with the same name get
`_2`, `_3`, ... in node ID order. `Name` keeps the original browse name. All collections (`Machines`, `SkillSet`,
`Components`, variable groups, ...) are keyed by segment.

```csharp
var pick = (RemoteSkill)server.Resolve("/M1/SkillSet/Pick");
var maybe = server.ResolveOptional("/M1/SkillSet/Drop");   // null
var skills = server.Find("/M1/**/SkillSet/*");             // in tree order
var grip = gripper.Resolve("SkillSet/Grip");               // relative to an object
var nested = gripper.Find("**/SkillSet/*");
```

`Find` takes a glob: `*`, `?` and `[...]` match within one segment, `**` matches any number of segments.
`ObjectPath.Escape` turns a segment into a pattern that matches only itself.

### Shared variables

A variable can be listed in two groups, e.g. a parameter that is also shown in `Monitoring`. It is still one object.
The group that references it with `HasComponent` owns it and gives its `Path`; the group that references it with
`Organizes` lists it under an alias (`Aliases`), and `Resolve` finds it there too. `Find` lists it once.

## Browse rules

Browse rules decide per path which parts of an object are browsed. All matching rules add their features.

```csharp
BrowseRule[] rules =
[
    BrowseRules.When(Match.All, BrowseFeature.Machines, BrowseFeature.Monitoring),
    BrowseRules.When(Match.Name("DummyMachine"), BrowseFeature.Components, BrowseFeature.SkillSet),
];
```

Matchers are `Match.All`, `Match.Name`, `Match.Path`, `Match.PathPrefix`, `Match.Glob` and the combinators
`Match.Any`, `Match.AllOf` and `Match.Not`. Machines are checked at `/<machine>`, users at `/`. A string path that is
not valid throws when the rule is made.

## Direct node IDs

With `BrowseRules = []` the client browses nothing. Build objects from known node IDs instead:

```csharp
var component = await RemoteComponent.CreateAsync(server, "DummyComponent",
    NodeId.Parse("ns=1;s=DummyMachine.Components.DummyComponent"), browseRules: [BrowseRule.Default]);
```

Without a parent, its path is `/Components/DummyComponent`.
