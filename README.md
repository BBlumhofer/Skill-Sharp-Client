# OpenSmi.Client

C#-Client für OPC-UA-Server nach dem Smart Machine Interface (SMI), etwa `opensmi-server`. Die
Bibliothek portiert den Python-Client `opensmi-client` (Stand `e8740c2`) und bildet dessen
Objektmodell ab: Maschinen, Komponenten, Ressourcen, Skills, Methoden, Lock, User, Variablen,
Notifications, History und DTOs. Sie nutzt das OPC-Foundation-SDK
(`OPCFoundation.NetStandard.Opc.Ua.Client`) und läuft auf `net8.0` und `net10.0`. Die Beispiele
brauchen `net10.0`.

Der alte `SkillSharp.Client` liegt nur noch zum Nachschlagen in `legacy/` und wird nach dem Merge
gelöscht.

## Stand gegenüber dem Python-Client

Umgesetzt sind Connect mit Retry, Browsen mit und ohne BrowseRules, Direct-Node-ID, Variablen
lesen, schreiben und abonnieren (auch eigene Enum-Typen des Servers), Skills mit Start, Reset, Halt,
Suspend und Waits, Methoden, Lock und `AutoLock`, User, Notifications, History, Security mit
Trust-Store, Reconnect mit erhaltenen Objekten und DTOs.

Vom Python-Client fehlt nur der Fallback auf den Knoten `Messages` älterer v4-Server. Er kommt mit
dem SmartFactory-Paket (Ports, Storages, Recovery), das ein eigenes Vorhaben wird. Ebenfalls offen
ist ein erneuter Versuch für Monitored Items, die der Server beim Anlegen abgelehnt hat (SPEC §7.1);
das kann auch der Python-Client nicht.

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
await skill.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["x"] = 5, ["y"] = 10 });
await skill.StartAsync(StartWait.Completion, TimeSpan.FromSeconds(30));
Console.WriteLine(string.Join(", ", await skill.FinalResultData.ReadAllAsync()));
await skill.ResetAsync(wait: true);
```

`DummySkill` startet nur, wenn die `StartupSkill` läuft. Das Beispiel `example` zeigt, wie man sie
hochfährt.

## Änderungen beobachten

Für Zustand, Werte und Status gibt es Events mit async Handlern:

```csharp
skill.StateChanged += async (sender, e) => await Console.Out.WriteLineAsync($"{((RemoteSkill)sender).Name}: {e.State}");
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

`WatchStateAsync` (Skill), `WatchValueAsync` (Variable) und `WatchStatusAsync` (Server) liefern zuerst
den aktuellen Wert und danach jede Änderung. Jeder Aufrufer hat einen kleinen Puffer. Wer zu langsam
liest, verliert die ältesten Werte, bekommt aber immer den neuesten.

## Worauf man achten muss

Die gecachten Werte (`CurrentState`, `Value`, Lock-Zustand) füllt die Subscription. Direkt nach
`ConnectAsync` kann dort noch der Anfangswert stehen, bei Skills also `Halted`. Wer gleich danach
entscheidet, liest den Wert mit `ReadCurrentStateAsync` oder `ReadValueAsync` vom Server.

`opensmi-server` bemerkt eine geschlossene Session erst nach bis zu einer Sekunde und gibt dann den
Lock ihres Users frei. Das trifft auch einen Lock, den derselbe User inzwischen über eine neue
Session geholt hat. Wer sich mit demselben User schnell neu verbindet, wartet also kurz oder holt
den Lock mit `AutoLock`.

## Beispiele

Alle Python-Beispiele aus `opensmi-client/examples/` liegen in einem Konsolenprojekt. Der erste
Parameter wählt das Beispiel, der zweite die Server-URL (optional):

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
| `quick_start` | führt `ExampleSkill` aus dem Server `opensmi-server/examples/quick_start` (Port 4841) aus |
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

Die Integrationstests laufen gegen den echten Python-`opensmi-server`. Nötig sind Python 3.12 mit
installiertem `opensmi-server` und dessen Ordner `examples/dummy`. Die Erwartungswerte stammen vom
Python-Client gegen opensmi-server `main` (29e4d4a). Mit älteren Serverständen weicht der
DTO-Vergleich um zwei Variablen ab.

- `OPENSMI_SERVER_DIR`: Pfad zu `opensmi-server/examples/dummy`. Ohne die Variable sucht die
  Testsuite unter `../opensmi-server/examples/dummy` neben dem Repo.
- `OPENSMI_TEST_PORT`: Port des gemeinsamen Testservers (Standard 4843). Läuft dort schon ein
  Server, nutzen die Tests ihn mit, sonst starten sie selbst einen.

Einige Testklassen (Beispiele, Security, Reconnect) starten eigene Server auf festen Ports in
eigenen Temp-Verzeichnissen. Die CI (`.github/workflows/ci.yml`) führt die Tests bei Push und PR
auf Ubuntu aus, mit .NET 8 und 10 und opensmi-server auf festem Commit.

## Dokumentation

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
