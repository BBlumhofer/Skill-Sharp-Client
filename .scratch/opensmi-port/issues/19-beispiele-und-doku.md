# 19 Beispiele, README, DEVIATIONS.md

Type: task
Status: resolved
Blocked by: 17, 20

## Question

Die Python-Beispiele aus `opensmi-client/examples/` (Referenz-Checkout, siehe Map) nach `examples/`
portieren, in **einem** Konsolenprojekt `examples/OpenSmi.Client.Examples` mit dem Beispielnamen als
erstem Argument (`dotnet run -- quick_start`). Dazu kommen ein README im Repo-Root (Ersatz für die
UTF-16-Datei) und `docs/opensmi/DEVIATIONS.md` mit jeder Abweichung aus SPEC §10, dem Stand im Code
und dem Test, der sie belegt (fehlt ein Test, gehört einer dazu). `security_policy` folgt erst mit
Ticket 21; bis dahin steht es im README als „folgt“.

## Akzeptanz

- jedes Beispiel läuft gegen einen `TestServer` ohne Exception durch; ein Integrationstest ruft die
  Beispiel-Einstiegspunkte direkt auf (keine Prozesse)
- `DEVIATIONS.md` verweist für jede Zeile auf einen existierenden Testnamen

## Answer

Alle 12 Beispiele liegen in `examples/OpenSmi.Client.Examples`, je eine Klasse `<Name>Example` mit
`RunAsync(url, …, CancellationToken)`; `Program.cs` wählt per erstem Argument. `security_policy` kam
nach Ticket 21 doch noch dazu (Basic256Sha256 statt Pythons Aes256Sha256RsaPss, wie vom Koordinator
vorgegeben). Beispiele, die in Python endlos oder 120 s laufen, nehmen eine optionale `duration`.
`quick_start` nimmt Maschinen- und Skillnamen als Parameter, weil der Test den Dummy-Server statt des
quick_start-Servers nutzt. `example.py` heißt in C# `ColdStartExample`.

Tests: `ExamplesTests` (11 Beispiele, eigener Server auf Port 4872), `SecurityPolicyExampleTests`
(verschlüsselter Server auf 4873), `DeviationTests` (Timeout/Cancel beim Warten, die einzige
Abweichung ohne Test). `DEVIATIONS.md` führt 11 Zeilen (die Security-Zeile aus SPEC §10 ist in
Zertifikat und Trust-Store geteilt), alle umgesetzt und mit existierendem Testnamen. Das UTF-16-README
ist ersetzt.
