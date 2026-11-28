# Variables

Variables live in groups: `Identification`, `Attributes`, `Monitoring` and `ParameterSet` on machines, components and
resources, and `ParameterSet`, `Monitoring` and `FinalResultData` on skills and methods. A group is a read-only
dictionary of `RemoteVariable`, keyed by segment.

## Samples

The client caches a `Sample<object?>` per variable and keeps it current through the subscription.

| Member | Meaning |
|---|---|
| `Value` | converted value, `null` without one |
| `Variant` | raw OPC UA value, `null` without one |
| `Status`, `Quality` | status code and its severity: `Good`, `Uncertain` or `Bad` |
| `SourceTimestamp`, `ServerTimestamp`, `Timestamp` | `Timestamp` is the source timestamp, or the server timestamp if there is none |

Until the first value arrives the status is `BadWaitingForInitialData`. `variable.Value`, `RawValue` and `Timestamp`
read from the cached sample. Skill and component states use the same type (`Sample<SkillState>`,
`Sample<MachineryItemState?>`).

## Reading

```csharp
var force = machine.ParameterSet["DummyFloatParameter"];
object? cached = force.Value;
object? fresh = await force.ReadValueAsync();                   // throws on a bad status
Sample<object?> sample = await force.ReadSampleAsync();          // never throws on a bad status
var all = await machine.ParameterSet.ReadAllAsync();             // one request for the whole group
```

`ReadValueAsync(updateCache: true)` and `ReadSampleAsync(updateCache: true)` also update the cache.

## Writing

```csharp
await force.WriteValueAsync(3);   // converted to the variable's type, here Double
await machine.ParameterSet.WriteAllAsync(new Dictionary<string, object?>
{
    ["DummyStringParameter"] = "batch", ["DummyIntParameter"] = 7,
});
```

The value is converted to the type of the cached value: numbers between numeric types, a string to `NodeId` or
`LocalizedText`. `WriteAllAsync` writes in one request; an unknown name throws `KeyNotFoundException` before anything
is written. A rejected value throws `OpenSmiInvalidValueException`; a missing lock or user right throws
`OpenSmiAccessDeniedException`. `IsWritable` comes from the user access level of the logged-in user.

## Metadata

| Member | Meaning |
|---|---|
| `UaDataType` | name of the data type, e.g. `Double` or a custom type name |
| `UaValueRank` | `-1` scalar, `0` one or more dimensions, `n` exactly n dimensions |
| `Unit`, `Range` | `EngineeringUnits` and `EURange`, if the variable has them |
| `ValidValues` | allowed values with their display names, see below |

`ValidValues` comes from two places. For a variable whose value is a node ID, it maps the node IDs of all
`Utilizes` targets to their display names, plus `""` for none. For a custom enum data type, it maps each number to the
member name. `Value` then is the display name or member name, and `WriteValueAsync` accepts the name as well as the
key. Custom structures stay `ExtensionObject`.

`variable.Unit?.ToUnit()` maps the `EUInformation` to the generated `Unit` enum (2133 units from opensmi-core, PascalCase names
such as `Unit.Centimetre`); `Unit.Centimetre.ToEuInformation()` goes the other way.

## History

```csharp
var history = await variable.ReadHistoryAsync(numValues: 10);   // at most 10 samples
```

`ReadHistoryAsync(start, end, numValues, returnBounds)` returns samples, including entries with a bad status.
`numValues: 0` reads everything.
