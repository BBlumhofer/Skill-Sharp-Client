# 30 Custom-Datentypen des Servers (custom_type_definitions)

Type: task
Status: resolved
Blocked by: –

## Question

Python lädt beim Connect die Custom-Datentypen des Servers (`load_data_type_definitions`,
`RemoteServer.custom_type_definitions`). `RemoteVariable` bildet IntEnum-Typen auf Enum-Member ab
(`remote_variable.py:150-163, 185-205`) und füllt damit `valid_values`. Der C#-Client kennt das noch
nicht (siehe 09). Mit dem SDK geht das über `Opc.Ua.Client.ComplexTypes` (`ComplexTypeSystem.LoadAsync`).
Offen ist, ob das Paket im Cache liegt, und welcher Server Custom-Typen für einen ehrlichen Test hat
(keiner der Beispielserver `dummy`, `quick_start`, `dictionary_entries` hat welche; `rsl` startet nicht).

## Entscheidung (Nutzer, 2026-10-29)

Jetzt bauen und testen, mit einem eigenen kleinen Custom-Enum-Typ, der nur im Testserver-Verzeichnis
(Kopie des Dummy-Servers im `TestServer`) hinzukommt, nicht in opensmi-server.

## Answer

Umgesetzt ohne ComplexTypes-Paket: `asyncua.new_enum` (so legt opensmi-server `IntEnum`-Variablen an,
`_server.py:ensure_enum`) schreibt nur `DataTypeDefinition` (`EnumDefinition`), das liest `RemoteVariable`
für jeden DataType mit Namespace > 0 zusammen mit dem BrowseName. Ergebnis wie Python (Orakel geprüft):
`UaDataType = "TestColor"`, `ValidValues = {"0": "Red", "1": "Green", "2": "Blue"}`, `Value = "Green"`
(Membername statt `IntEnum`, DTO bleibt JSON-freundlich). Schreiben per Name (`"Blue"`) oder Zahl (`0`);
Python scheitert beim Namen an der asyncua-Serialisierung. Unbekannter Name → `OpenSmiInvalidValueException`.
Lesefehler der Definition nur Warnung. Structs bleiben `ExtensionObject` (nicht im Scope).
Test: `CustomTypeTests` mit `TestServer.StartAsync(port, customTypes: true)`, das `custom_types.py` in die
Dummy-Kopie legt und `DummyComponent.ParameterSet.DummyEnumParameter` ergänzt. Abweichungen in
`docs/opensmi/DEVIATIONS.md`.
