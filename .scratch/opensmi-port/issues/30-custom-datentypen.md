# 30 Custom data types of the server (custom_type_definitions)

Type: task
Status: resolved
Blocked by: none

## Question

On connect, Python loads the server's custom data types (`load_data_type_definitions`,
`RemoteServer.custom_type_definitions`). `RemoteVariable` maps IntEnum types to enum members
(`remote_variable.py:150-163, 185-205`) and fills `valid_values` with them. The C# client does not know
this yet (see 09). With the SDK this works through `Opc.Ua.Client.ComplexTypes` (`ComplexTypeSystem.LoadAsync`).
Open: whether the package is in the cache, and which server has custom types for an honest test
(none of the example servers `dummy`, `quick_start`, `dictionary_entries` has any; `rsl` does not start).

## Decision (user, 2026-10-29)

Build and test it now, with a small custom enum type of our own that is added only in the test server directory
(the copy of the dummy server in `TestServer`), not in opensmi-server.

## Answer

Implemented without the ComplexTypes package: `asyncua.new_enum` (this is how opensmi-server creates `IntEnum` variables,
`_server.py:ensure_enum`) writes only `DataTypeDefinition` (`EnumDefinition`); `RemoteVariable` reads that
for every DataType with namespace > 0, together with the BrowseName. Result as in Python (checked against the oracle):
`UaDataType = "TestColor"`, `ValidValues = {"0": "Red", "1": "Green", "2": "Blue"}`, `Value = "Green"`
(member name instead of `IntEnum`, the DTO stays JSON-friendly). Writing by name (`"Blue"`) or number (`0`);
Python fails on the name because of the asyncua serialization. Unknown name → `OpenSmiInvalidValueException`.
A read error of the definition is only a warning. Structs stay `ExtensionObject` (not in scope).
Test: `CustomTypeTests` with `TestServer.StartAsync(port, customTypes: true)`, which puts `custom_types.py` into the
dummy copy and adds `DummyComponent.ParameterSet.DummyEnumParameter`. Deviations in
`docs/opensmi/DEVIATIONS.md`.
