# OpenSmi.Client

C#-Client für OPC-UA-Server nach dem Smart Machine Interface (SMI), z. B. `opensmi-server`. Die
Bibliothek ist ein Port des Python-Clients `opensmi-client` (Stand `9026539`, opensmi-core 0.7.1) und bildet
dessen Objektmodell ab: Maschinen, Komponenten, Ressourcen, Skills mit Checks, Methoden, Lock, User, Variablen,
Notifications und History. Sie baut auf dem OPC-Foundation-SDK (`OPCFoundation.NetStandard.Opc.Ua.Client`) auf
und zielt auf `net8.0` und `net10.0` (die Beispiele nur auf `net10.0`).

**Version 2.0.0** ersetzt den alten `SkillSharp.Client` (1.x) vollständig; dessen Code ist aus dem Repo entfernt
(letzter Stand: Commit `e57b3c0`). Was sich gegenüber 1.x und der ersten Portfassung ändert, steht in
[CHANGELOG.md](CHANGELOG.md).

## Stand gegenüber dem Python-Client

Umgesetzt: Connect mit Retry, Browsen mit und ohne BrowseRules (auch als Glob), Direct-Node-ID, Pfade mit
`Resolve`/`Find`, Variablen lesen, schreiben und abonnieren (als `Sample` mit Status und Zeitstempeln), Skills
(Start/Reset/Halt/Suspend mit Waits) und ihre Checks, Methoden, Lock und `AutoLock`, User des Servers,
Notifications mit Fehlercode, History, Security (Zertifikat und Trust-Store), Reconnect mit erhaltenen Objekten.

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
await skill.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["X"] = 5, ["Y"] = 10 });
await skill.StartAsync(StartWait.Completion, TimeSpan.FromSeconds(30));
Console.WriteLine(string.Join(", ", await skill.FinalResultData.ReadAllAsync()));
await skill.ResetAsync(wait: true);
```

`DummySkill` braucht eine laufende `StartupSkill`. Wie man sie hochfährt, zeigt das Beispiel `example`.

## Pfade

Jedes Objekt hat einen Pfad wie im Python-Client: Elternpfad, OpenSMI-Ordner (`Components`, `Resources`,
`SkillSet`, `MethodSet`, `Users`) und Segment.

```csharp
var pick = (RemoteSkill)server.Resolve("/DummyMachine/SkillSet/DummySkill");
var skills = server.Find("/DummyMachine/**/SkillSet/*");        // Glob: *, ?, [...], ** über Ebenen
var nested = machine.Find("Components/*/SkillSet/*");           // relativ zu einem Objekt
var check = pick.Path / "FeasibilityCheck";                     // Checks liegen unter ihrem Skill
var user = server.Users["operator"];                            // /Users/operator
```

Das Segment ist der Browse-Name. Namen mit `/` oder leere Namen werden bereinigt (`_`), doppelte Namen bekommen
in NodeId-Reihenfolge `_2`, `_3`, … Die Collections (`SkillSet`, `Components`, `Machines`, Variablengruppen, …)
sind nach Segment geschlüsselt. Eine Variable, die in zwei Gruppen steht, ist ein Objekt: Ihr Pfad liegt beim
Besitzer (Referenz `HasComponent`), die andere Stelle (`Organizes`) ist ein Alias (`Aliases`) und löst über
`Resolve` ebenfalls auf. BrowseRules arbeiten auf denselben Pfaden: `Match.Path`, `Match.PathPrefix`,
`Match.Name` und `Match.Glob`.

## Werte und Zustände

Variablen cachen ein `Sample<object?>` (`Value`, `Variant`, `Status`, `Quality`, `SourceTimestamp`,
`ServerTimestamp`). Bis zum ersten Wert hat es den Status `BadWaitingForInitialData`. `ValueChanged` kommt nur,
wenn sich Wert oder Status ändern, nicht bei einem neuen Zeitstempel allein. `ReadHistoryAsync` liefert Samples.

Skills haben `CurrentState` (`SkillState`, ohne Wert `Halted`) und `CurrentStateSample`; Maschinen und Komponenten
haben `CurrentState` als `MachineryItemState?` (`NotAvailable`, `OutOfService`, `NotExecuting`, `Executing`),
`null` ohne CurrentState-Knoten (Ressourcen). Beide Zustände sind nach `ConnectAsync` bereits gelesen.

## Änderungen beobachten

Zustand, Werte und Status lassen sich auf zwei Arten verfolgen. Events mit async Handlern:

```csharp
skill.StateChanged += async (sender, e) =>
    await Console.Out.WriteLineAsync($"{((RemoteSkill)sender).Name}: {e.State} ({e.Sample.Quality})");
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

`WatchStateAsync` (Skill), `WatchValueAsync` (Variable, als `Sample`) und `WatchStatusAsync` (Server) liefern
zuerst den aktuellen Wert, danach jede Änderung. Jeder Aufrufer hat einen kleinen Puffer; wer zu langsam liest,
verliert die ältesten Werte, bekommt aber immer den neuesten.

## Beispiele

Alle Python-Beispiele aus `opensmi-client/examples/` (Stand 9026539; `dto` ist dort entfallen) liegen in einem Konsolenprojekt. Der erste
Parameter wählt das Beispiel, der zweite (optional) die Server-URL:

```
dotnet run --project examples/OpenSmi.Client.Examples -- example
dotnet run --project examples/OpenSmi.Client.Examples -- example opc.tcp://localhost:4843/server
```

| Beispiel | Inhalt |
|---|---|
| `auto_lock` | hält den Lock der Maschine mit `AutoLock`, bis Strg+C |
| `browse_rules` | browst nur, was die BrowseRules verlangen |
| `direct_component` | verbindet ohne Browsen und baut eine Komponente aus ihrer NodeId |
| `example` | Lock, `StartupSkill` hochfahren, `DummySkill` ausführen |
| `history` | liest die letzten 10 Werte einer historisierten Variable |
| `method_call` | ruft `DummyMethod` auf |
| `quick_start` | führt `ExampleSkill` aus; erwartet den Server aus `opensmi-server/examples/quick_start` auf `opc.tcp://localhost:4841/server` |
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

Die Integrationstests laufen gegen den echten Python-`opensmi-server` 0.3.1, nicht gegen Mocks. Nötig
sind Python 3.12+ (Befehl `python`) mit installiertem `opensmi-server` und dessen Beispielordner `examples/`.

- `OPENSMI_SERVER_DIR`: Pfad zu `opensmi-server/examples/dummy`. Ohne die Variable sucht die
  Testsuite unter `../opensmi-server/examples/dummy` neben dem Repo. Die anderen Beispiele werden
  daneben (`examples/check`, `dictionary_entries`, `quick_start`, `rsl`) erwartet.

Gegen jeden Beispielserver von opensmi-server prüft `ServerExamplesTests`, dass der Client dieselben Objekte
wie der Python-Client findet (Orakel in `tests/OpenSmi.Client.Tests/oracle/`, erzeugt mit `dump_paths.py`) und
dass das Beispiel fachlich läuft (Skill mit Feasibility-Check, Quick-Start-Skill, Dictionary-Eintrag, RSL-Komponente).
`servers/run_example.py` startet ein Beispiel unverändert auf einem eigenen Port; `servers/paths_server.py` ist
der Testserver der Upstream-Tests (alle Objektarten, Sonderfälle bei Namen und Besitz, altes User-Layout).
- `OPENSMI_TEST_PORT`: Port des gemeinsamen Testservers (Standard 4843). Läuft dort schon ein
  Server, wird er mitbenutzt, sonst startet die Testsuite selbst einen.

Einige Testklassen (Beispiele, Security, Neustart) starten eigene Server auf festen Ports in
eigenen Temp-Verzeichnissen.

CI: `.github/workflows/ci.yml` führt die Tests bei Push und PR auf Ubuntu aus (.NET 8 und 10, opensmi-server `v0.3.1`).

opensmi-server bemerkt geschlossene Sessions nur einmal pro Sekunde und gibt dann den Lock des *Users* frei,
auch wenn inzwischen eine neue Session desselben Users ihn hält. Die Tests warten deshalb nach jedem Test kurz
(`ServerFixture.SettleAsync`); in Anwendungen gilt dasselbe beim schnellen Neuverbinden mit demselben User.

## Dokumentation

- [docs/opensmi/SPEC.md](docs/opensmi/SPEC.md): Spezifikation und Abnahmekriterien
- [docs/opensmi/UMBAUPLAN.md](docs/opensmi/UMBAUPLAN.md): Umbauplan in Phasen
- [CHANGELOG.md](CHANGELOG.md): Änderungen in 2.0.0
- [docs/opensmi/DEVIATIONS.md](docs/opensmi/DEVIATIONS.md): bewusste Abweichungen vom Python-Client
- [docs/opensmi/analyse/](docs/opensmi/analyse/): Inventare des Python-Clients und des alten Clients
