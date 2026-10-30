# Umbauplan: Skill-Sharp-Client → OpenSMI-Client für C#

Stand: 2026-10-29. Zielbild: [SPEC.md](SPEC.md). Belege: [analyse/](analyse/)
([OI] = OpenSMI-Inventar, [SI] = Skill-Sharp-Inventar).

## 0. Ausgangslage in Kürze

- **Python-Vorbild:** `opensmi-client`, rund 3.000 Zeilen, sauber geschichtet (Server, Machine,
  Component, Skill, Method, Variable, Lock, AutoLock, BrowseRules, DTOs), stützt sich auf
  `opensmi-core` (Enums, SubscriptionManager, Signals) [OI §1–2].
- **C#-Bestand:** `SkillSharp.Client`, rund 6.900 Zeilen, davon zwei God-Classes:
  `RemoteModule` mit 2.127 und `RemoteSkill` mit 1.508 Zeilen. Der Build läuft mit 54 Warnungen
  durch [SI §1–2].
- **Die Klassennamen stimmen fast 1:1 mit dem Vorbild überein, die Implementierung nicht.** Der
  C#-Client weicht in Kernpunkten ab bzw. ist fehlerhaft [SI §2]:
  - Lock-Ownership-Heuristik: jeder SSC-Client hält fremde Locks für eigene.
  - `InitLockAsync` kann den Lock freigeben.
  - `UaClient.CallMethodAsync` liefert immer `null`.
  - `RemoteMethod.CallAsync` wirft für die meisten Instanzen.
  - Discovery geht nur eine Ebene tief.
  - Subscriptions: kein Dedup, Re-Read bei jeder Notification, Events nach Reconnect verloren.
  - Reconnect erzeugt neue Modulobjekte.
  - Hunderte leere `catch {}`.
- **Kein Konsument:** IMAPRO hat die Referenz entfernt (Commits `b764bd6`, `fc21f7a`) und
  simuliert die Hardware [SI §7]. Auf API-Kompatibilität muss keine Rücksicht genommen werden.

**Konsequenz für die Strategie:** Ein schrittweises Refactoring der bestehenden Klassen lohnt
sich nicht. Fast jede Kernmethode müsste ohnehin neu geschrieben werden, und die Fehler stecken
gerade in der Struktur (Heuristiken, Doppelmechanismen). Stattdessen:

> **Neuer Kern als sauberer Port von `opensmi-client` im selben Repo. Der alte Code bleibt nur
> als Wissensquelle (OPC-UA-SDK-Handhabung, SmartFactory-Spezifika) und wird nach
> Funktionsparität archiviert.**

Wiederverwendet werden Wissen und einzelne Snippets: Endpoint-Auswahl, KeepAlive/Reconnect-Handler,
Event-Parsing, Port-, Storage- und Nachbar-Logik für das SmartFactory-Paket. Die Klassen selbst
werden nicht übernommen.

## 1. Aufwand (revidiert)

Die erste Schätzung lag bei 2–4 Wochen und setzte voraus, dass der C#-Code weitgehend tragfähig
ist. Nach der Detailanalyse trifft das nicht zu.

| Phase | Inhalt | Aufwand (PT) |
|---|---|---|
| P0 | Repo-Hygiene, Sicherheit, Branch | 0,5–1 |
| P1 | Skelett, Test-Harness mit echtem `opensmi-server` | 2–3 |
| P2 | Verbindung, SubscriptionManager, Signals | 3–4 |
| P3 | Browse-Modell, Variablen, Container, BrowseRules | 4–5 |
| P4 | Skills, Methoden, Lock, AutoLock, Users, Resources | 4–5 |
| P5 | DTOs, Notifications, History, Security, Beispiele, Doku | 3–4 |
| **Summe Kern** | **Funktionsparität mit opensmi-client** | **≈ 17–22 PT (4–5 Wochen)** |
| P6 (optional) | SmartFactory-Paket (Ports, Storages, Recovery) | 5–8 |
| P7 | Altcode archivieren, Release 0.1.0 | 0,5–1 |

Die größten Unsicherheiten: das Verhalten des OPC-Foundation-SDK beim Reconnect bzw. Subscription-
Transfer (P2) und offene Fragen zum Informationsmodell wie die Lage von `Requirements` (P1/P4).

## 2. Phasen im Detail

Jede Phase endet mit grünem Build ohne Warnungen und grünen Tests. Integrationstests laufen ab P1
gegen den echten `opensmi-server`.

### P0 – Hygiene und Sicherheit (vor jeder Codeänderung)

1. **Lokales Repo reparieren.** Im lokalen `.git` fehlt die Datei `HEAD`; `refs/heads/main` steht
   auf `9a0a6c2`, GitHub ist mit `e57b3c0` neuer, und es gibt einen lokalen Stash `c19961d`
   (Inhalt ungeprüft). Vorschlag, manuell auszuführen:
   ```
   cd C:\Users\BenjaminBlumhofer\Repos\Skill-Sharp-Client
   echo ref: refs/heads/main> .git\HEAD
   git fetch origin
   git stash show -p stash@{0}      # prüfen, ob darin etwas Wichtiges steckt
   git status                       # Arbeitsbaum = Kopie von e57b3c0
   git reset --soft origin/main     # nur wenn nichts Lokales verloren geht
   ```
2. Branch `opensmi-port` anlegen.
3. `.gitignore` (Standard für .NET) hinzufügen. Aus dem Index entfernen: die 190 `bin/`/`obj/`-
   Dateien, `UAClient.Tests/.vs/` (inkl. Copilot-Indizes, `.suo`) und `TestResults/*.trx`
   (enthalten Nutzer- und Hostnamen) [SI §6].
4. **Zugangsdaten:** `Tools/RemoteInspector/config.json` enthält eine interne IP und
   `visitor/visitor`, Tests und `Program.cs` enthalten `orchestrator/orchestrator` und
   `172.24.100.85`. Diese Werte durch Umgebungsvariablen ersetzen. Falls es echte Zugangsdaten
   sind, die Passwörter in der SmartFactory ändern lassen, denn sie stehen in der öffentlichen
   Git-Historie.
5. README neu schreiben (die alte Datei ist UTF-16 und enthält nur den Titel), `LICENSE` (MIT)
   ergänzen. Encoding-Probleme beheben (`BaseRemoteCallable.cs` in cp1252, U+FFFD in
   `RemotePort.cs`).

### P1 – Skelett und Test-Harness

1. Neue Solution `OpenSmi.Client.slnx` mit `src/OpenSmi.Client`, `tests/OpenSmi.Client.Tests`,
   `tests/OpenSmi.Client.IntegrationTests` und `examples/`. Gemeinsame
   `Directory.Build.props`: `Nullable`, `TreatWarningsAsErrors`, Analyzer (`CA2007`, `CA2016`,
   leere Catch-Blöcke als Fehler).
2. **Test-Fixture `OpenSmiServerFixture`:** startet `python -m opensmi.server` mit dem
   Dummy-Server aus `opensmi-server` (Python-venv, `pip install opensmi-server` in fester Version)
   auf einem freien Port und wartet, bis der Endpoint erreichbar ist. Dazu ein
   `scripts/setup-test-server.ps1` und ein GitHub-Actions-Workflow (Windows und Linux).
3. **Erkundungstest:** den Adressraum des Dummy-Servers browsen und als Snapshot (`*.verified.txt`)
   ablegen. Damit sind die offenen Fragen aus SPEC §6 (Lage von `Requirements`, Namespaces)
   entschieden, bevor Code darauf aufbaut.
4. Den alten Code nach `legacy/` verschieben (git mv) und aus der neuen Solution herausnehmen. Er
   bleibt zum Nachschlagen erhalten.

### P2 – Verbindung, Subscriptions, Signals

1. `RemoteServerOptions`, `RemoteServer.ConnectAsync`/`DisconnectAsync`/`DisposeAsync`,
   Retry-Logik und Statusabbildung [OI §2.2]. Session über `DefaultSessionFactory` erzeugen, ohne
   Reflection.
2. Reconnect ausschließlich über den `SessionReconnectHandler`. **Test:** Server neu starten bzw.
   die Verbindung trennen. Danach müssen `Machines` und alle Objektinstanzen identisch sein und
   Notifications weiter eintreffen.
3. `SubscriptionManager`: eine Subscription, Dedup je NodeId, Chunking, Heartbeat mit Rebuild
   inklusive Events, Retry fehlgeschlagener Items [SPEC §7.1].
4. `AsyncSignal<T>` und den seriellen Dispatch-Channel je Server einführen. Unit-Tests für
   Reihenfolge, Ausnahme-Isolation und Disconnect während des Sendens.
5. Namespace-Map, `GetNamespaceIndex`, Server-Objektregister mit Weak-Referenzen.

### P3 – Browse-Modell und Variablen

1. `RemoteUaObject` mit Lebenszyklus (`InitAsync` idempotent, Rollback bei Fehler).
2. `BrowseFeature`, `BrowseRule`, die Matcher und die Pfadberechnung. Reine Unit-Tests anhand der
   Beispiele aus [OI §3].
3. `BaseRemoteComponent`-Dispatch nach exaktem BrowseName, rekursive Komponenten, Resources,
   `MachineryItemState`, gebatchte Browse- und Read-Aufrufe, `InitExceptions`.
4. `RemoteVariable` (Read mit DataType, ValueRank, UserAccessLevel; Unit/Range; ValidValues über
   `Utilizes`; Custom-/Enum-Typen über die `DataTypeSystem`-Unterstützung des SDK) und
   `WriteValueAsync` mit Typkonvertierung.
5. Variable-Container mit gebatchtem `ReadAllAsync`/`WriteAllAsync`.
6. Integrationstests: `browse_rules.py` und `direct_component.py` nachbauen, Snapshot-Vergleich
   der DTO-Bäume mit der Ausgabe des Python-Clients gegen denselben Server. Das ist der stärkste
   Paritätstest, weil die Python-Version als Orakel dient.

### P4 – Skills, Methoden, Lock

1. `RemoteCallable` (Container, `MinAccessLevel`, `Requires`-Abhängigkeiten).
2. `RemoteSkill`: Methoden-NodeIds beim Init auflösen, `ObjectId = StateMachine`, Wait-Logik mit
   Halt-Zähler, `StartWait.Completion`, Timeout und Token, `Suspend`, Feasibility- und
   Precondition-Checks.
3. `RemoteMethod.CallAsync` auf `Call`.
4. `RemoteLock` mit exakten Methodennamen und `LockResult`, Renew-Due-Logik [OI §6]. `AutoLock`.
5. `RemoteUser` (mit `LockPriority`), `RemoteMachine.Users`.
6. Integrationstests: `quick_start`, `example`, `method_call`, `auto_lock`. Dazu Lock-Konflikte
   mit zwei Clients und unterschiedlichen Usern (`operator` gegen `orchestrator`, `BreakLock`
   durch den höher priorisierten User, AutoLock-Relock) sowie Zugriff ohne Lock
   (`BadRequiresLock` → `OpenSmiAccessDeniedException`).

### P5 – Abrundung

1. DTOs als `record`s mit `System.Text.Json`-Serialisierung (ISO-Zeitstempel, Pfade als String);
   Beispiel `dto`.
2. Notifications (EventFilter, `|||`-Parsing, Fallback auf `Messages`), Beispiel
   `subscription_component`.
3. History (`HistoryRead`), Beispiel `history`.
4. Security mit persistentem Zertifikat und Trust-Store, Beispiel `security_policy` gegen den
   Server mit aktivierter Verschlüsselung.
5. `subscription_server` und `subscription_variable` nachbauen.
6. README, XML-Doku für das gesamte öffentliche API, `DEVIATIONS.md` mit allen Abweichungen aus
   SPEC §10 und den zugehörigen Tests.

### P6 – SmartFactory-Paket (optional)

Nur wenn reale SmartFactory-KL-Module angebunden werden sollen (z. B. für den
Ausführungsagenten aus Diss Kap. 3).

1. `OpenSmi.Client.SmartFactory` als eigenes Projekt.
2. Port-, Storage- und Slot-Wrapper, `NeighborResolver`, `ResourceTypeResolver`, `ModuleReadiness`
   und `ModuleRecoveryService` [SPEC §9]. Die Logik kommt aus `legacy/` (`RemotePort.cs`,
   `RemoteStorage*.cs`, `RemoteModule.cs:82-133, 1106-1302, 1566-2125`), bereinigt um Heuristiken
   und Doppelmechanismen.
3. Tests: reine Funktionen (RFID-Parsing, Resource-Pfade) als Unit-Tests; Hardware-Tests nur
   hinter `RUN_SMARTFACTORY_TESTS=1` und ohne hart kodierte Adressen.

### P7 – Abschluss

1. `legacy/` löschen und den letzten alten Stand als Tag `skillsharp-legacy-final` markieren.
2. Version `0.1.0`, NuGet-Paket (lokaler Feed bzw. GitHub Packages).
3. Optional: das Repo umbenennen (`opensmi-client-dotnet`) und bei SmartFactory-KL wegen einer
   Aufnahme als offizieller C#-Client anfragen.

## 3. Zuordnung alt → neu

| Alt (`legacy/UAClient/...`) | Neu | Umgang |
|---|---|---|
| `UaClient.cs` | `RemoteServer` (intern `SessionHost`) | Endpoint-Auswahl übernehmen; Reflection, Default-Subscription, `Task.Delay`, hart kodierte Zertifikatsakzeptanz entfernen |
| `RemoteServer.cs` | `RemoteServer` | Statusmodell erweitern, Polling-Reconnect entfernen, keine neuen Objekte nach Reconnect |
| `RemoteModule.cs` | `RemoteMachine` + SF-Paket | generischer Teil neu nach [OI §2.5]; Ports, Storage, Nachbarn, Startup, Recovery → P6 |
| `RemoteComponent.cs` | `BaseRemoteComponent`/`RemoteComponent` | neu; Event-Parsing ist korrekt umzusetzen |
| `RemoteSkill.cs` | `RemoteSkill` | neu; `ExecuteAsync` entfällt (Orchestrierung ist Aufgabe des Aufrufers, IMAPRO nutzte es auch nicht [SI §7]) |
| `BaseRemoteCallable.cs`, `RemoteMethod.cs` | `RemoteCallable`, `RemoteMethod` | neu |
| `RemoteVariable.cs`, `RemoteVariableCollector.cs` | `RemoteVariable`, `RemoteVariableContainer` | neu; kein rekursives Flachklopfen nach DisplayName |
| `RemoteLock.cs`, `RemoteLockDescriptor.cs` | `RemoteLock`, `AutoLock` | neu |
| `SubscriptionManager.cs` | `SubscriptionManager` (intern) | neu |
| `SessionBrowseCache.cs`, `UaHelpers.cs` | entfällt / interne Helfer | gebatchte Browses statt Pfad-Cache |
| `RemotePort.cs`, `RemoteStorage*.cs` | SF-Paket | P6 |
| `Common/Enums.cs` | `SkillState` | übernehmen und um `MachineryItemState`, `MachineryOperationMode` ergänzen |
| `Common/Exceptions.cs` | §8 der Spec | ersetzen (Py-Namen entfallen) |
| `Common/Log.cs` | `ILogger` | entfällt |
| `Program.cs`, `LockTest.cs`, `Tools/RemoteInspector` | `examples/`, optional `tools/OpenSmi.Inspector` | aus der Library entfernen |
| `UAClient.Tests/*` | `tests/*` | Unit-Tests neu; die Integrationstests gegen SF-Hardware gehen nach P6 |

## 4. Risiken

| Risiko | Wirkung | Gegenmaßnahme |
|---|---|---|
| SDK-Reconnect überträgt Subscriptions anders als erwartet | Notifications fehlen nach Reconnect | P2-Test mit Server-Neustart; Heartbeat-Rebuild als Rückfallebene |
| Python-Server nicht im CI startbar (Windows-Runner, Abhängigkeiten) | keine Integrationstests | früh in P1 klären; notfalls Linux-Runner oder Docker-Image |
| Upstream ändert das Informationsmodell (Projekt ist „Beta“) | Brüche | feste Versionen von Server und Client in der Fixture; Snapshot-Test aus P1 zeigt Änderungen an |
| Reale SF-Server weichen vom opensmi-server ab (z. B. `ModuleSet`, alte `Messages`) | Kern findet nichts | Fallbacks im SF-Paket; einmaliger Abgleich mit realem Modul vor P6 |
| Zeitbedarf neben der Diss | Port bleibt halbfertig | P0–P3 liefern bereits einen nutzbaren Lese-Client; P4 ist der nächste sinnvolle Haltepunkt |

## 5. Nächste Schritte

1. Die offenen Entscheidungen aus SPEC §11 treffen (Paketname, net8, SF-Paket ja/nein).
2. P0 Schritt 1 ausführen (lokales Repo reparieren, Stash prüfen).
3. P1 beginnen: Fixture mit `opensmi-server` und Adressraum-Snapshot.
