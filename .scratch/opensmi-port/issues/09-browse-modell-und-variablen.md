# 09 Browse-Modell, Container, Variablen (lesen)

Type: task
Status: resolved
Blocked by: 07, 08

## Question

`RemoteUaObject` (Name, Path, NodeId, UaType, Parent, Register), `BaseRemoteComponent`-Dispatch nach
exaktem BrowseName, rekursive Komponenten, Resources, `MachineryItemState`, Variable-Container
(`IReadOnlyDictionary`) und `RemoteVariable` mit Read von DataType, ValueRank, UserAccessLevel,
Unit/Range und ValidValues. Werte werden per Subscription aktualisiert. Browse-Einstieg über
Machinery i=1001 mit Fallback. `BrowseRules = []` → keine Discovery; dazu `RemoteComponent.CreateAsync`
(Direct-Node-ID). Orakel: `remote_component.py`, `remote_variable*.py`.

## Akzeptanz (Integration)

- `Machines["DummyMachine"].Components["DummyComponent"].Components["NestedDummyComponent"]` existiert
- Pfade wie im Orakel, z. B. `/DummyMachine/ParameterSet/DummyFloatParameter` mit `IsWritable`
  und `UaDataType == "Double"`
- `UaValueRank == -1` für Skalare (SPEC §10)
- Regel `Match.Name("DummyMachine")` + Components → Komponenten vorhanden, aber kein SkillSet bei `DummyComponent`
- `BrowseRules = []` → `Machines` leer; `CreateAsync` mit `ns=1;s=DummyMachine.Components.DummyComponent` → `Path == "/DummyComponent"`

## Answer

`RemoteUaObject.cs`, `RemoteComponent.cs`, `RemoteVariable.cs`, `RemoteVariableContainer.cs`. Die Reihenfolge der Dictionaries ist nicht Teil des Vertrags (Python füllt in Completion-Reihenfolge). Custom-Enum-Typen (`custom_type_definitions`) sind nicht portiert, der Dummy-Server hat keine. Tests: `BrowseModelTests`.
