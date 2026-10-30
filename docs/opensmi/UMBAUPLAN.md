# Umbauplan: Skill-Sharp-Client → OpenSMI-Client für C#

Hinweis: Dieses Dokument wurde mit KI-Unterstützung (Claude, Anthropic) erstellt; siehe den KI-Hinweis im README.

Status 2026-10-30: Die Phasen P0 bis P5 sind erledigt. Der Kern liegt in `src/OpenSmi.Client`, die
Tests in `tests/OpenSmi.Client.Tests` laufen in der CI unter Linux gegen opensmi-server `29e4d4a`
grün. Offen sind P6 (SmartFactory-Paket) und P7 (Abschluss). Den Verlauf mit allen Entscheidungen
hält die Map fest: `.scratch/opensmi-port/map.md` mit den Tickets unter `issues/`. Zielbild:
[SPEC.md](SPEC.md), Abweichungen vom Python-Client: [DEVIATIONS.md](DEVIATIONS.md), Belege:
[analyse/](analyse/) ([OI] = OpenSMI-Inventar, [SI] = Skill-Sharp-Inventar).

## Ausgangslage und Strategie

- Python-Vorbild: `opensmi-client`, rund 3.000 Zeilen, sauber geschichtet (Server, Machine,
  Component, Skill, Method, Variable, Lock, AutoLock, BrowseRules, DTOs), stützt sich auf
  `opensmi-core` (Enums, SubscriptionManager, Signals) [OI §1–2].
- C#-Bestand: `SkillSharp.Client`, rund 6.900 Zeilen, davon zwei God-Classes:
  `RemoteModule` mit 2.127 und `RemoteSkill` mit 1.508 Zeilen. Der Build lief mit 54 Warnungen
  durch [SI §1–2].
- Die Klassennamen stimmten fast 1:1 mit dem Vorbild überein, die Implementierung nicht. Der
  C#-Client wich in Kernpunkten ab bzw. war fehlerhaft [SI §2]:
  - Lock-Ownership-Heuristik: jeder SSC-Client hielt fremde Locks für eigene.
  - `InitLockAsync` konnte den Lock freigeben.
  - `UaClient.CallMethodAsync` lieferte immer `null`.
  - `RemoteMethod.CallAsync` warf für die meisten Instanzen.
  - Die Discovery ging nur eine Ebene tief.
  - Subscriptions: kein Dedup, Re-Read bei jeder Notification, Events nach Reconnect verloren.
  - Ein Reconnect erzeugte neue Modulobjekte.
  - Hunderte leere `catch {}`.
- Kein Konsument: IMAPRO hat die Referenz entfernt (Commits `b764bd6`, `fc21f7a`) und
  simuliert die Hardware [SI §7]. Auf API-Kompatibilität musste niemand Rücksicht nehmen.

Ein schrittweises Refactoring der bestehenden Klassen lohnte sich deshalb nicht. Fast jede
Kernmethode hätte ohnehin neu geschrieben werden müssen, und die Fehler steckten in der Struktur
(Heuristiken, Doppelmechanismen). Entschieden wurde:

> Neuer Kern als sauberer Port von `opensmi-client` im selben Repo. Der alte Code bleibt nur
> als Wissensquelle (OPC-UA-SDK-Handhabung, SmartFactory-Spezifika) und wird nach
> Funktionsparität archiviert.

Wiederverwendet werden Wissen und einzelne Snippets: Endpoint-Auswahl, KeepAlive/Reconnect-Handler,
Event-Parsing, Port-, Storage- und Nachbar-Logik für das SmartFactory-Paket. Die Klassen selbst
wurden nicht übernommen. Der Altcode liegt bis P7 in `legacy/`.

Das lokale Repo war zu Beginn defekt (fehlendes `HEAD`, beschädigter Objektspeicher). Es wurde
aus `origin` neu aufgebaut, der Arbeitsbaum blieb unverändert (Ticket 04).

## Offen

### P6: SmartFactory-Paket

Ein eigenes Vorhaben nach dem Client (Ticket 03). Nur nötig, wenn reale SmartFactory-KL-Module
angebunden werden sollen (z. B. für den Ausführungsagenten aus Diss Kap. 3).

1. `OpenSmi.Client.SmartFactory` als eigenes Projekt.
2. Port-, Storage- und Slot-Wrapper, `NeighborResolver`, `ResourceTypeResolver`, `ModuleReadiness`
   und `ModuleRecoveryService` [SPEC §9]. Die Logik kommt aus `legacy/` (`RemotePort.cs`,
   `RemoteStorage*.cs`, `RemoteModule.cs:82-133, 1106-1302, 1566-2125`), bereinigt um Heuristiken
   und Doppelmechanismen. Dazu gehören der Einstieg über `SFM:ModuleSet` und der Fallback auf
   `Messages` für alte Server, die der Kern nicht hat.
3. Tests: reine Funktionen (RFID-Parsing, Resource-Pfade) als Unit-Tests; Hardware-Tests nur
   hinter `RUN_SMARTFACTORY_TESTS=1` und ohne hart kodierte Adressen.

### P7: Abschluss (Ticket 33)

1. Nach dem Merge von `opensmi-port` in `main`: den letzten alten Stand (`e57b3c0`) als Tag
   `skillsharp-legacy-final` markieren und `legacy/` löschen.
2. Version `0.1.0`, NuGet-Paket und eine Umbenennung des Repos (`opensmi-client-dotnet`) warten,
   bis klar ist, ob SmartFactory-KL den Client als offiziellen C#-Client aufnimmt.

## Zuordnung alt → neu

| Alt (`legacy/UAClient/...`) | Neu | Umgang |
|---|---|---|
| `UaClient.cs` | `RemoteServer` | Endpoint-Auswahl übernommen; Reflection, Default-Subscription, `Task.Delay`, hart kodierte Zertifikatsakzeptanz entfernt |
| `RemoteServer.cs` | `RemoteServer` | Statusmodell erweitert, Polling-Reconnect entfernt, keine neuen Objekte nach Reconnect |
| `RemoteModule.cs` | `RemoteMachine` + SF-Paket | generischer Teil neu nach [OI §2.5]; Ports, Storage, Nachbarn, Startup, Recovery → P6 |
| `RemoteComponent.cs` | `BaseRemoteComponent`/`RemoteComponent` | neu, Event-Parsing korrigiert |
| `RemoteSkill.cs` | `RemoteSkill` | neu; `ExecuteAsync` entfällt (Orchestrierung ist Aufgabe des Aufrufers, IMAPRO nutzte es auch nicht [SI §7]) |
| `BaseRemoteCallable.cs`, `RemoteMethod.cs` | `RemoteCallable`, `RemoteMethod` | neu |
| `RemoteVariable.cs`, `RemoteVariableCollector.cs` | `RemoteVariable`, `RemoteVariableContainer` | neu; kein rekursives Flachklopfen nach DisplayName |
| `RemoteLock.cs`, `RemoteLockDescriptor.cs` | `RemoteLock`, `AutoLock` | neu |
| `SubscriptionManager.cs` | `SubscriptionManager` (intern) | neu |
| `SessionBrowseCache.cs`, `UaHelpers.cs` | interne Helfer (`UaUtil`) | gebatchte Browses statt Pfad-Cache |
| `RemotePort.cs`, `RemoteStorage*.cs` | SF-Paket | P6 |
| `Common/Enums.cs` | `SkillState` in `Enums.cs` | neu nach `opensmi-core` |
| `Common/Exceptions.cs` | §8 der Spec | ersetzt (Py-Namen entfallen) |
| `Common/Log.cs` | `ILogger` | entfällt |
| `Program.cs`, `LockTest.cs`, `Tools/RemoteInspector` | `examples/OpenSmi.Client.Examples` | aus der Library entfernt |
| `UAClient.Tests/*` | `tests/OpenSmi.Client.Tests` | Tests neu; Integrationstests gegen SF-Hardware gehören zu P6 |

## Risiken und wie sie ausgingen

| Risiko | Wirkung | Gegenmaßnahme | Ergebnis |
|---|---|---|---|
| SDK-Reconnect überträgt Subscriptions anders als erwartet | Notifications fehlen nach Reconnect | Test mit Server-Neustart; Heartbeat-Rebuild als Rückfallebene | Das SDK klont die Subscriptions bei einer neuen Session; der SubscriptionManager hängt sich um, die Objekte bleiben. Beides ist mit Server-Neustart und kurzem Netzabbruch getestet (Ticket 22, 23, 28). |
| Python-Server nicht im CI startbar (Windows-Runner, Abhängigkeiten) | keine Integrationstests | früh klären; notfalls Linux-Runner oder Docker-Image | Die CI läuft auf einem Linux-Runner mit Python 3.12 und ist grün (Ticket 32). |
| Upstream ändert das Informationsmodell (Projekt ist „Beta“) | Brüche | feste Versionen von Server und Client | CI fest auf opensmi-server `29e4d4a`; `DtoTests` vergleicht die Pfade mit der Ausgabe des Python-Clients. |
| Reale SF-Server weichen vom opensmi-server ab (z. B. `ModuleSet`, alte `Messages`) | Kern findet nichts | Fallbacks im SF-Paket; einmaliger Abgleich mit realem Modul vor P6 | offen, gehört zu P6 |
| Zeitbedarf neben der Diss | Port bleibt halbfertig | P0 bis P3 liefern bereits einen nutzbaren Lese-Client | Der Kern ist vollständig (P0 bis P5). |
