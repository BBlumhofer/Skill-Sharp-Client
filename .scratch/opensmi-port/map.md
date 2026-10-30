# Map: Skill-Sharp-Client → OpenSMI-Client für C#

Label: wayfinder:map

## Destination

Eine C#-Bibliothek `OpenSmi.Client`, die sich gegenüber dem `opensmi-server` verhält wie der
Python-Client `opensmi-client @ e8740c2`. Abnahme: SPEC §12
([docs/opensmi/SPEC.md](../../docs/opensmi/SPEC.md)).

## Notes

- **Ausführung gehört zu dieser Map** (Abweichung vom Wayfinder-Default): Der Nutzer hat
  verlangt, die Tickets danach per TDD umzusetzen. Implementierungs-Tickets haben `Type: task`
  (AFK) und werden mit den Skills `tdd` und `ponytail` bearbeitet.
- **Orakel ist immer der Python-Client.** Referenz-Checkout:
  `C:\Users\BenjaminBlumhofer\Repos\CA-Modul\opensmi-port\opensmi-client` (e8740c2). Bei Fragen
  zum Verhalten gilt zuerst der Code dort, dann [OI](../../docs/opensmi/analyse/opensmi-client-inventar.md),
  danach erst SPEC. Abweichungen gibt es nur, wenn sie in SPEC §10 stehen.
- **Testserver:** `cd ..\opensmi-server\examples\dummy; python main.py` →
  `opc.tcp://localhost:4843/server`. User (= Passwort, Priorität): visitor 0, planner 5,
  orchestrator 10, remote 15, operator 20, developer 100.
- **Python-Orakel** gegen denselben Server:
  `$env:PYTHONPATH="<Referenz-Checkout>\src"; python oracle.py`. Das installierte
  `OpenSMI-Client 1.0.0rc2` passt nicht zum installierten Core und lässt sich nicht verwenden.
- **Seams (TDD):**
  1. Öffentliche API von `OpenSmi.Client` gegen den echten `opensmi-server`
     (Integrationstests). Das ist der Haupt-Seam, das SDK wird nicht gemockt.
  2. Reine Funktionen ohne Server: BrowseRules/Matcher, Zustands-Parsing, Notification-Parsing,
     `AsyncSignal`. Hier laufen Unit-Tests.
- Ponytail: Was die Python-Version nicht hat und SPEC nicht verlangt, wird nicht gebaut.
- **Parallele Agenten:** Jeder Agent bekommt einen eigenen Port (`OPENSMI_TEST_PORT`, eigene
  `TestServer`-Ports) und ein eigenes Build-Verzeichnis (`dotnet test OpenSmi.Client.slnx
  --artifacts-path .artifacts/<name>`). Agenten fassen keine Dateien eines anderen offenen Tickets an.

## Decisions so far

- [01 Lage von `Requirements`](issues/01-requirements-lage.md): Liegt an Skill **und**
  `SkillExecution`. Wie Python wird nur `SkillExecution` gelesen, die Suche an zwei Stellen entfällt.
- [02 Testserver lokal startbar](issues/02-testserver-lokal.md): Ja, Python 3.12 und
  opensmi-server sind editable installiert, Endpoint `/server` auf Port 4843.
- [04 Repo-Reparatur](issues/04-repo-reparatur-und-zugangsdaten.md): `.git` neu aus origin, Branch
  `opensmi-port`; die Passwörter sind nur Testzugänge.
- [05 Skelett](issues/05-skelett-und-server-fixture.md): ein Testprojekt, die Fixture nutzt einen
  laufenden Server mit. Der Altcode liegt in `legacy/`.
- [06 RemoteServer](issues/06-remote-server-connect.md): `DefaultSessionFactory` ohne Reflection,
  vorerst nur NoSecurity.
- [07 Signals/Subscriptions](issues/07-signals-und-subscriptions.md): serieller Channel-Dispatch;
  kein Abbestellen, das Chunking erledigt das SDK.
- [08 BrowseRules](issues/08-browse-rules.md): 1:1 aus `browse.py`.
- [09 Browse-Modell](issues/09-browse-modell-und-variablen.md): Die Reihenfolge der Dictionaries
  ist kein Vertrag, Custom-Enum-Typen sind nicht portiert.
- [10 Schreiben](issues/10-variablen-schreiben.md): Batch-Write mit Einzel-Fallback.
- [11 RemoteSkill](issues/11-remote-skill.md): Der Server prüft den Zustand vor dem Lock.
- [12 RemoteMethod](issues/12-remote-method.md): wie Python.
- [13 RemoteLock](issues/13-remote-lock.md): `BreakLock` übernimmt den Lock; `ExitLock` ohne Lock
  gibt AccessDenied.
- [14 AutoLock](issues/14-auto-lock.md): wie Python.
- [15 RemoteUser](issues/15-remote-user.md): `LockPriority`.
- [16 Notifications](issues/16-notifications.md): ohne den Fallback auf `Messages`.
- [17 DTOs](issues/17-dto.md): Parität mit dem Python-Orakel (113 Objekte) ist getestet.
- [18 History, Security, Reconnect](issues/18-history-security-reconnect.md): History ist erledigt,
  der Rest aufgeteilt in 21, 22 und 23.
- [19 Beispiele und Doku](issues/19-beispiele-und-doku.md): alle 12 Python-Beispiele in einem
  Konsolenprojekt (`example.py` → `ColdStartExample`), README neu, `DEVIATIONS.md` mit Test je Zeile.
- [20 Eigener Testserver](issues/20-eigener-testserver.md): `TestServer` mit eigenem Port und
  optionaler Verschlüsselung, Stop/Restart; der Port des Collection-Servers per `OPENSMI_TEST_PORT`.
- [03 Offene Entscheidungen](issues/03-offene-entscheidungen-spec11.md): bleibt im Repo, zusätzlich
  net8.0, das SF-Paket kommt danach, `Unit` wird generiert; das Signalmodell wird neu bewertet.
- [21 Security](issues/21-security.md): `SecurityOptions` mit Ordner-Stores, Client-Zertifikat
  einmal erzeugt, exakte Endpoint-Wahl; ein nicht vertrauenswürdiger Server bricht ohne Retry ab.
- [22 Reconnect im SDK](issues/22-reconnect-im-sdk.md): Ein Recreate liefert eine neue Session mit
  geklonten Subscriptions; `RemoteServer.Session` und `SubscriptionManager` müssen umhängen,
  die Objekte bleiben. Backoff 1 s bis 30 s.
- [23 Reconnect](issues/23-reconnect.md): KeepAlive → `Reconnecting`, neue Session wird übernommen,
  `SubscriptionManager.Rebind`, Objekte bleiben identisch; Heartbeat baut Data- und Event-Items neu auf.
  Reste in 28.
- [27 Best Practice Benachrichtigungen](issues/27-benachrichtigung-best-practice.md): Netzwerk-Clients
  (MQTTnet 4, RabbitMQ 7, Azure IoT v2, SignalR, NATS) nutzen async Events plus `IAsyncEnumerable` zum
  Warten; Rx ist kaum verbreitet. Empfohlen werden async Events, Watch-Streams und WaitFor.
- [24 Multi-Target](issues/24-multi-target-net8.md): `net8.0;net10.0`, Beispiele nur net10.0,
  Test-Targets laufen nacheinander.
- [25 Benachrichtigungsmodell](issues/25-benachrichtigungsmodell.md): async Events (nacheinander),
  Watch-Streams als `IAsyncEnumerable`, WaitFor bleibt; kein Rx; Unit-Enum in PascalCase. Umsetzung in 29.
- [28 Reconnect-Reste](issues/28-reconnect-reste.md): `WatchdogInterval` (1 s), ohne AutoReconnect
  → `Disconnected`, kurzer Netzabbruch per TcpProxy getestet. Item-Retry übersprungen (nicht ehrlich testbar).
- [30 Custom-Datentypen](issues/30-custom-datentypen.md): Enum-DataTypes über `DataTypeDefinition`
  (ohne ComplexTypes-Paket); `Value` = Membername, Schreiben per Name oder Zahl; Test mit eigenem Testtyp.
- [31 Disconnect absichern](issues/31-disconnect-absichern.md): gleichzeitige `DisconnectAsync` teilen
  einen Abbau; ein Aufruf aus dem `Disconnecting`-Handler kehrt sofort zurück.
- [32 CI](issues/32-ci.md): GitHub Actions auf Linux, opensmi-server fest auf main 29e4d4a; noch nie gelaufen.
- [29 Benachrichtigungsmodell umgesetzt](issues/29-benachrichtigungsmodell-umsetzen.md): `AsyncEvents.cs`
  (Delegate, EventArgs, serielles Raise, Watch-Puffer 8/DropOldest); `AsyncSignal` entfernt; Units in PascalCase.
- [26 Unit-Enum](issues/26-unit-enum.md): 2133 Einheiten generiert aus opensmi-core
  (`tools/generate_units.py`), Python-Namen (`CENTIMETRE`), Helfer in `Units`, Abbildung per `eu.ToUnit()`.

## Not yet specified

- Abgleich gegen die Beispielserver von opensmi-server (2026-10-29): `quick_start` (19 Objekte) und
  `dictionary_entries` (15) sind identisch mit dem Python-Client. `rsl` startet wegen eines Server-Fehlers
  nicht auf dem lokalen Feature-Branch 547ce5b; auf opensmi-server `main` (29e4d4a) läuft `rsl`, und
  Python und C# liefern dieselben 19 Objekte.
- Retry einzelner fehlgeschlagener Monitored Items (SPEC §7.1): bisher kein realistischer Auslöser
  gegen den Dummy-Server; erst ticketfähig, wenn ein echter Server das Problem zeigt.
- Release 0.1.0, NuGet und Repo-Umbenennung: warten auf die Frage, ob SmartFactory-KL den Client aufnimmt.

## Out of scope

- SmartFactory-Paket (Ports, Storages, Nachbarn, Recovery): eigenes Vorhaben nach dem Client
  ([03](issues/03-offene-entscheidungen-spec11.md)).
- Server-Implementierung, Port von `opensmi-core` jenseits des Client-Bedarfs (SPEC §1).
- API-Kompatibilität zum alten `SkillSharp.Client` (kein Konsument mehr, SPEC §1).

## Offene Punkte auf GitHub (ab 2026-10-30)

Was nach dem PR [#2](https://github.com/BBlumhofer/Skill-Sharp-Client/pull/2) offen ist, wird als
GitHub-Issue weitergeführt, nicht mehr hier:
[#3 LICENSE/NOTICE](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/3), [#4 leeres catch](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/4), [#5 Lock-Bug in opensmi-server](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/5),
[#6 Tag und legacy/ löschen (Ticket 33)](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/6), [#7 Item-Retry](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/7), [#8 SmartFactory-Paket](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/8),
[#9 Release/NuGet/Umbenennung](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/9), [#10 CI-Actions](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/10), [#11 Test NodeId-Schreiben](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/11),
[#12 Completing nach Completed](https://github.com/BBlumhofer/Skill-Sharp-Client/issues/12).

