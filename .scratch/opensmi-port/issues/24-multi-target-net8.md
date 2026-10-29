# 24 Multi-Target net8.0 und net10.0

Type: task
Status: open
Blocked by: 19, 21, 23

## Question

`OpenSmi.Client` baut für `net8.0;net10.0` (Entscheidung 03). Die Tests laufen gegen beide Targets.
Bekannte Stolpersteine: `System.Threading.Lock` gibt es erst ab .NET 9 (für net8 auf `object`
ausweichen, ohne `#if`, wenn es geht), `Lock`-Felder in `AsyncSignal`, `RemoteUaObject` und
`RemoteSkill`. SDK 8.0.423 ist installiert. Die Beispiele bleiben `net10.0`. Wartet, bis die
laufenden Agenten fertig sind, weil `Directory.Build.props` alle Builds betrifft.

## Akzeptanz

- `dotnet build` ohne Warnungen für beide Targets
- die Suite ist grün für `net8.0` und `net10.0`
