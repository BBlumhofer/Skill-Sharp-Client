# 30 Custom-Datentypen des Servers (custom_type_definitions)

Type: task
Status: open
Blocked by: –

## Question

Python lädt beim Connect die Custom-Datentypen des Servers (`load_data_type_definitions`,
`RemoteServer.custom_type_definitions`). `RemoteVariable` bildet IntEnum-Typen auf Enum-Member ab
(`remote_variable.py:150-163, 185-205`) und füllt damit `valid_values`. Der C#-Client kennt das noch
nicht (siehe 09). Mit dem SDK geht das über `Opc.Ua.Client.ComplexTypes` (`ComplexTypeSystem.LoadAsync`).
Offen ist, ob das Paket im Cache liegt, und welcher Server Custom-Typen für einen ehrlichen Test hat
(keiner der Beispielserver `dummy`, `quick_start`, `dictionary_entries` hat welche; `rsl` startet nicht).
