# 09 Browse model, containers, variables (reading)

Type: task
Status: resolved
Blocked by: 07, 08

## Question

`RemoteUaObject` (Name, Path, NodeId, UaType, Parent, Register), `BaseRemoteComponent` dispatch by
exact BrowseName, recursive components, resources, `MachineryItemState`, variable containers
(`IReadOnlyDictionary`) and `RemoteVariable` with a read of DataType, ValueRank, UserAccessLevel,
Unit/Range and ValidValues. Values are updated by subscription. Browse entry point via
Machinery i=1001 with a fallback. `BrowseRules = []` → no discovery; for that, `RemoteComponent.CreateAsync`
(direct node ID). Oracle: `remote_component.py`, `remote_variable*.py`.

## Acceptance (integration)

- `Machines["DummyMachine"].Components["DummyComponent"].Components["NestedDummyComponent"]` exists
- paths as in the oracle, e.g. `/DummyMachine/ParameterSet/DummyFloatParameter` with `IsWritable`
  and `UaDataType == "Double"`
- `UaValueRank == -1` for scalars (SPEC §10)
- rule `Match.Name("DummyMachine")` + Components → components present, but no SkillSet on `DummyComponent`
- `BrowseRules = []` → `Machines` empty; `CreateAsync` with `ns=1;s=DummyMachine.Components.DummyComponent` → `Path == "/DummyComponent"`

## Answer

`RemoteUaObject.cs`, `RemoteComponent.cs`, `RemoteVariable.cs`, `RemoteVariableContainer.cs`. The order of the dictionaries is not part of the contract (Python fills them in completion order). Custom enum types (`custom_type_definitions`) are not ported; the dummy server has none. Tests: `BrowseModelTests`.
