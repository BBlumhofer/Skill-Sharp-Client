# OpenSmi.Client

C#-Client für OPC-UA-Server nach dem Smart Machine Interface (SMI), etwa `opensmi-server`. Die
Bibliothek portiert den Python-Client `opensmi-client` (Stand `9026539`, opensmi-core 0.7.1) und bildet
dessen Objektmodell ab: Maschinen, Komponenten, Ressourcen, Skills mit Checks, Methoden, Lock, User,
Variablen, Notifications und History. Sie nutzt das OPC-Foundation-SDK
(`OPCFoundation.NetStandard.Opc.Ua.Client`) und läuft auf `net8.0` und `net10.0`. Die Beispiele
brauchen `net10.0`.

**Version 2.0.0** ersetzt den alten `SkillSharp.Client` 1.x vollständig und erscheint unter demselben
Paketnamen `SkillSharp.Client`; Namespace und Assembly heißen `OpenSmi.Client`. Der alte Code ist aus dem
Repo entfernt, sein letzter Stand trägt den Tag `skillsharp-legacy-final` (`e57b3c0`). Was sich ändert,
steht in [CHANGELOG.md](CHANGELOG.md).

## Stand gegenüber dem Python-Client

Umgesetzt sind Connect mit Retry, Browsen mit und ohne BrowseRules (auch als Glob), Direct-Node-ID,
Pfade mit `Resolve`/`Find`, Variablen lesen, schreiben und abonnieren (als `Sample` mit Status und
Zeitstempeln, auch eigene Enum-Typen des Servers), Skills mit Start, Reset, Halt, Suspend und Waits und
ihre Checks, Methoden mit ihren Abhängigkeiten (`Requirements`), Lock und `AutoLock`, User des Servers,
Notifications mit Fehlercode, History, Security mit Trust-Store und Reconnect mit erhaltenen Objekten.

Vom Python-Client fehlt nur der Fallback auf den Knoten `Messages` älterer v4-Server. Er kommt mit
dem SmartFactory-Paket (Ports, Storages, Recovery), das ein eigenes Vorhaben wird (Issue #8). Ebenfalls
offen ist ein erneuter Versuch für Monitored Items, die der Server beim Anlegen abgelehnt hat
(SPEC §7.1, Issue #7); das kann auch der Python-Client nicht.

Wo der C#-Client absichtlich anders arbeitet als Python, steht mit Test in
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

`DummySkill` startet nur, wenn die `StartupSkill` läuft. Das Beispiel `example` zeigt, wie man sie
hochfährt.

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

Für Zustand, Werte und Status gibt es Events mit async Handlern:

```csharp
skill.StateChanged += async (sender, e) =>
    await Console.Out.WriteLineAsync($"{((RemoteSkill)sender).Name}: {e.State} ({e.Sample.Quality})");
```

Die Handler laufen nacheinander in Anmeldereihenfolge auf der Benachrichtigungsschleife des Clients.
Wirft einer, wird das geloggt, und der nächste läuft trotzdem. Ein Handler sollte kurz sein und darf
nicht auf eine spätere Benachrichtigung desselben Clients warten, etwa mit
`StartAsync(StartWait.Completion)`. Diese Benachrichtigung kommt erst, wenn der Handler fertig ist,
und das Warten endet im Timeout. Zum Warten sind die Streams und die `WaitFor…Async`-Methoden da:

```csharp
await foreach (var state in skill.WatchStateAsync(cancellationToken))
    if (state == SkillState.Completed) break;
```

`WatchStateAsync` (Skill), `WatchValueAsync` (Variable, als `Sample`) und `WatchStatusAsync` (Server)
liefern zuerst den aktuellen Wert und danach jede Änderung. Jeder Aufrufer hat einen kleinen Puffer. Wer
zu langsam liest, verliert die ältesten Werte, bekommt aber immer den neuesten.

## Worauf man achten muss

Skill- und Maschinenzustand liest der Client beim Verbinden einmal vom Server; Variablenwerte kommen mit
dem Browsen. Danach hält die Subscription die Caches aktuell, mit kurzer Verzögerung. Wer sofort auf den
exakten Serverstand angewiesen ist, liest mit `ReadCurrentStateAsync` oder `ReadValueAsync`.

`opensmi-server` bemerkt eine geschlossene Session erst nach bis zu einer Sekunde und gibt dann den
Lock ihres Users frei. Das trifft auch einen Lock, den derselbe User inzwischen über eine neue
Session geholt hat (Issue #5). Wer sich mit demselben User schnell neu verbindet, wartet also kurz oder
holt den Lock mit `AutoLock`.

## Beispiele

Alle Python-Beispiele aus `opensmi-client/examples/` liegen in einem Konsolenprojekt (`dto` ist upstream
entfallen). Der erste Parameter wählt das Beispiel, der zweite die Server-URL (optional):

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
| `quick_start` | führt `ExampleSkill` aus dem Server `opensmi-server/examples/quick_start` aus (`opc.tcp://localhost:4841/server`) |
| `security_policy` | verbindet mit Basic256Sha256 und SignAndEncrypt (siehe unten) |
| `subscription_component` | gibt 120 s lang Notifications und Statuswechsel aus |
| `subscription_server` | gibt Statuswechsel und Serverzeit aus, bis Strg+C |
| `subscription_variable` | gibt 120 s lang Wertänderungen aus |

Die übrigen Beispiele erwarten die Dummy-Maschine aus `opensmi-server/examples/dummy`
(`python main.py`, Endpoint `opc.tcp://localhost:4843/server`) mit den Testusern `visitor`,
`operator` und `orchestrator`. Das Passwort ist jeweils der Name.

`security_policy` legt das Client-Zertifikat in `pki/` an. Das Server-Zertifikat muss in
`pki/trusted/certs` liegen, sonst lehnt der Client die Verbindung ab. Im Code schaltet
`autoAcceptServerCertificate: true` die Prüfung ab; das ist nur für Tests gedacht. Der
Python-Client prüft das Server-Zertifikat gar nicht.

## Tests

```
dotnet test OpenSmi.Client.slnx
```

Die Integrationstests laufen gegen den echten Python-`opensmi-server` 0.3.1, nicht gegen Mocks. Nötig
sind Python 3.12 oder neuer (Befehl `python`) mit installiertem `opensmi-server` und dessen Ordner
`examples/`. Die Erwartungswerte (`tests/OpenSmi.Client.Tests/oracle/`) erzeugt der Python-Client
(9026539) mit `oracle/dump_paths.py` gegen dieselben Server.

- `OPENSMI_SERVER_DIR`: Pfad zu `opensmi-server/examples/dummy`. Ohne die Variable sucht die
  Testsuite unter `../opensmi-server/examples/dummy` neben dem Repo. Die anderen Beispiele werden
  daneben (`examples/check`, `dictionary_entries`, `quick_start`, `rsl`) erwartet.
- `OPENSMI_TEST_PORT`: Port des gemeinsamen Testservers (Standard 4843). Läuft dort schon ein
  Server, nutzen die Tests ihn mit, sonst starten sie selbst einen.

`ServerExamplesTests` startet jedes Beispiel von opensmi-server (`check`, `dictionary_entries`,
`quick_start`, `rsl`) unverändert auf einem eigenen Port (`servers/run_example.py`). Geprüft wird, dass
der Client dieselben Objekte findet wie der Python-Client und dass das Beispiel fachlich läuft:
Quick-Start-Skill, Skill mit Feasibility-Check (auch der abgelehnte Fall mit Fehlercode), Dictionary-Eintrag,
RSL-Komponente. `servers/paths_server.py` ist der Testserver der Upstream-Tests (alle Objektarten,
Sonderfälle bei Namen und Besitz, altes User-Layout).

Einige Testklassen (Beispiele, Security, Reconnect, Pfade) starten eigene Server auf festen Ports in
eigenen Temp-Verzeichnissen. Nach jedem Test gegen einen Server warten die Tests kurz
(`ServerFixture.SettleAsync`, siehe „Worauf man achten muss“). Die CI (`.github/workflows/ci.yml`)
führt die Tests bei Push und PR auf Ubuntu aus, mit .NET 8 und 10 und opensmi-server `v0.3.1`.

## Dokumentation

- [CHANGELOG.md](CHANGELOG.md): Änderungen in 2.0.0
- [docs/opensmi/SPEC.md](docs/opensmi/SPEC.md): Spezifikation des Clients
- [docs/opensmi/DEVIATIONS.md](docs/opensmi/DEVIATIONS.md): bewusste Abweichungen vom Python-Client
- [docs/opensmi/UMBAUPLAN.md](docs/opensmi/UMBAUPLAN.md): ursprünglicher Plan für den Umbau
- [docs/opensmi/analyse/](docs/opensmi/analyse/): Inventare des Python-Clients und des alten Clients
- [.scratch/opensmi-port/map.md](.scratch/opensmi-port/map.md): Entscheidungen und Tickets des Ports

## KI-Hinweis

Code, Tests und Dokumentation dieses Ports sind zum größten Teil mit KI erstellt, mit Claude
(Anthropic) über Claude Code. Der Maintainer hat die Arbeit gesteuert und die offenen Entscheidungen
getroffen; sie stehen in den Tickets unter `.scratch/opensmi-port/`. Commits mit KI-Anteil tragen
die Zeile `Co-Authored-By: Claude`.

Die Korrektheit stützt sich auf die automatischen Tests gegen `opensmi-server` und auf den Vergleich
mit dem Python-Client. Reale SmartFactory-Hardware deckt keiner davon ab. Prüfe den Code selbst,
bevor du ihn an echten Maschinen einsetzt.
