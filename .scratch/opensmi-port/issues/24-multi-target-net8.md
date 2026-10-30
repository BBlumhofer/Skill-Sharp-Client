# 24 Multi-Target net8.0 und net10.0

Type: task
Status: resolved
Blocked by: 29

## Question

`OpenSmi.Client` baut für `net8.0;net10.0` (Entscheidung 03). Die Tests laufen gegen beide Targets.
Bekannte Stolpersteine: `System.Threading.Lock` gibt es erst ab .NET 9 (für net8 auf `object`
ausweichen, ohne `#if`, wenn es geht), `Lock`-Felder in `AsyncSignal`, `RemoteUaObject` und
`RemoteSkill`. SDK 8.0.423 ist installiert. Die Beispiele bleiben `net10.0`. Wartet, bis die
laufenden Agenten fertig sind, weil `Directory.Build.props` alle Builds betrifft.

## Akzeptanz

- `dotnet build` ohne Warnungen für beide Targets
- die Suite ist grün für `net8.0` und `net10.0`

## Answer

Mit Zustimmung des Nutzers wurden zwei fehlende Pakete einmalig von nuget.org in den lokalen
Cache geholt (`System.Formats.Asn1` 9.0.9, `System.Diagnostics.DiagnosticSource` 10.0.0); die
NuGet-Konfiguration ist unverändert. `Directory.Build.props` hat jetzt `net8.0;net10.0`, die
Beispiele bleiben `net10.0`. Die Beispieltests laufen nur unter net10.0, die Lock-Felder sind
`object`. Die Test-Targets laufen nacheinander (`TestTfmsInParallel=false`), weil sie sich den
Dummy-Server und feste Ports teilen. Ergebnis: net8.0 66/66, net10.0 78/78.
