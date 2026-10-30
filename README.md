# OpenSmi.Client

C#-Client für OPC-UA-Server nach dem Smart Machine Interface (SMI), z. B. `opensmi-server`. Die
Bibliothek ist ein Port des Python-Clients `opensmi-client` (Stand `e8740c2`) und bildet dessen
Objektmodell ab: Maschinen, Komponenten, Ressourcen, Skills, Methoden, Lock, User, Variablen,
Notifications, History und DTOs. Sie baut auf dem OPC-Foundation-SDK
(`OPCFoundation.NetStandard.Opc.Ua.Client`) auf und zielt auf `net8.0` und `net10.0` (die Beispiele nur auf `net10.0`).

Der alte `SkillSharp.Client` liegt nur noch als Wissensquelle in `legacy/`.

## Stand gegenüber dem Python-Client

Umgesetzt: Connect mit Retry, Browsen mit und ohne BrowseRules, Direct-Node-ID, Variablen lesen,
schreiben und abonnieren, Skills (Start/Reset/Halt/Suspend mit Waits), Methoden, Lock und
`AutoLock`, User, Notifications, History, Security (Zertifikat und Trust-Store), Reconnect mit
erhaltenen Objekten, DTOs.

Noch offen:

- erneuter Versuch einzelner fehlgeschlagener Monitored Items (SPEC §7.1)
- das SmartFactory-Paket (Ports, Storages, Recovery) als eigenes Vorhaben

Wo der C#-Client absichtlich anders arbeitet als Python, steht in
[docs/opensmi/DEVIATIONS.md](docs/opensmi/DEVIATIONS.md).

## Schnellstart

```csharp
using OpenSmi.Client;

await using var server = await new RemoteServer(new()
{
    Url = "opc.tcp://localhost:4843/server", Username = "operator", Password = "operator",
}).ConnectAsync();

var machine = server.Machines["DummyMachine"];
await machine.Lock.InitLockAsync();

var skill = machine.SkillSet["DummySkill"];
await skill.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["x"] = 5, ["y"] = 10 });
await skill.StartAsync(StartWait.Completion, TimeSpan.FromSeconds(30));
Console.WriteLine(string.Join(", ", await skill.FinalResultData.ReadAllAsync()));
await skill.ResetAsync(wait: true);
```

`DummySkill` braucht eine laufende `StartupSkill`. Wie man sie hochfährt, zeigt das Beispiel `example`.

## Änderungen beobachten

Zustand, Werte und Status lassen sich auf zwei Arten verfolgen. Events mit async Handlern:

```csharp
skill.StateChanged += async (sender, e) => await Console.Out.WriteLineAsync($"{((RemoteSkill)sender).Name}: {e.State}");
```

Die Handler laufen nacheinander in Anmeldereihenfolge auf der Benachrichtigungsschleife des Clients.
Wirft einer, wird das geloggt und der nächste läuft trotzdem. Ein Handler sollte kurz sein und darf
nicht auf eine spätere Benachrichtigung desselben Clients warten (z. B. `StartAsync(StartWait.Completion)`):
Die kommt erst, wenn der Handler fertig ist, und das Warten läuft in den Timeout. Dafür gibt es die
Streams und die `WaitFor…Async`-Methoden:

```csharp
await foreach (var state in skill.WatchStateAsync(cancellationToken))
    if (state == SkillState.Completed) break;
```

`WatchStateAsync` (Skill), `WatchValueAsync` (Variable) und `WatchStatusAsync` (Server) liefern zuerst
den aktuellen Wert, danach jede Änderung. Jeder Aufrufer hat einen kleinen Puffer; wer zu langsam liest,
verliert die ältesten Werte, bekommt aber immer den neuesten.

## Beispiele

Alle Python-Beispiele aus `opensmi-client/examples/` liegen in einem Konsolenprojekt. Der erste
Parameter wählt das Beispiel, der zweite (optional) die Server-URL:

```
dotnet run --project examples/OpenSmi.Client.Examples -- dto
dotnet run --project examples/OpenSmi.Client.Examples -- dto opc.tcp://localhost:4843/server
```

| Beispiel | Inhalt |
|---|---|
| `auto_lock` | hält den Lock der Maschine mit `AutoLock`, bis Strg+C |
| `browse_rules` | browst nur, was die BrowseRules verlangen |
| `direct_component` | verbindet ohne Browsen und baut eine Komponente aus ihrer NodeId |
| `dto` | gibt jede Maschine als DTO im JSON-Format aus |
| `example` | Lock, `StartupSkill` hochfahren, `DummySkill` ausführen |
| `history` | liest die letzten 10 Werte einer historisierten Variable |
| `method_call` | ruft `DummyMethod` auf |
| `quick_start` | führt `ExampleSkill` aus; erwartet den Server aus `opensmi-server/examples/quick_start` auf `opc.tcp://localhost:4841/` |
| `security_policy` | verbindet mit Basic256Sha256/SignAndEncrypt (siehe unten) |
| `subscription_component` | gibt 120 s lang Notifications und Statuswechsel aus |
| `subscription_server` | gibt Statuswechsel und Serverzeit aus, bis Strg+C |
| `subscription_variable` | gibt 120 s lang Wertänderungen aus |

Alle anderen Beispiele erwarten die Dummy-Maschine aus `opensmi-server/examples/dummy`
(`python main.py`, Endpoint `opc.tcp://localhost:4843/server`) mit den Usern `visitor`, `operator`
und `orchestrator` (Passwort gleich Name).

`security_policy` legt das Client-Zertifikat in `pki/` an. Das Server-Zertifikat muss in
`pki/trusted/certs` liegen, sonst lehnt der Client die Verbindung ab. Der Python-Client prüft das
Server-Zertifikat nicht. Im Code lässt sich die Prüfung mit `autoAcceptServerCertificate: true`
abschalten; das ist nur für Tests gedacht.

## Tests

```
dotnet test OpenSmi.Client.slnx
```

Die Integrationstests laufen gegen den echten Python-`opensmi-server`, nicht gegen Mocks. Nötig
sind Python 3.12 mit installiertem `opensmi-server` und dessen Beispielordner `examples/dummy`.

- `OPENSMI_SERVER_DIR`: Pfad zu `opensmi-server/examples/dummy`. Ohne die Variable sucht die
  Testsuite unter `../opensmi-server/examples/dummy` neben dem Repo.
- `OPENSMI_TEST_PORT`: Port des gemeinsamen Testservers (Standard 4843). Läuft dort schon ein
  Server, wird er mitbenutzt, sonst startet die Testsuite selbst einen.

Einige Testklassen (Beispiele, Security, Neustart) starten eigene Server auf festen Ports in
eigenen Temp-Verzeichnissen.

CI: `.github/workflows/ci.yml` führt die Tests bei Push und PR auf Ubuntu aus (.NET 8 und 10, opensmi-server auf festem Commit).

## Dokumentation

- [docs/opensmi/SPEC.md](docs/opensmi/SPEC.md): Spezifikation und Abnahmekriterien
- [docs/opensmi/UMBAUPLAN.md](docs/opensmi/UMBAUPLAN.md): Umbauplan in Phasen
- [docs/opensmi/DEVIATIONS.md](docs/opensmi/DEVIATIONS.md): bewusste Abweichungen vom Python-Client
- [docs/opensmi/analyse/](docs/opensmi/analyse/): Inventare des Python-Clients und des alten Clients
