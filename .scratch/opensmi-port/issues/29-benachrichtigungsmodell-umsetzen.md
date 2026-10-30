# 29 Benachrichtigungsmodell umsetzen

Type: task
Status: resolved
Blocked by: 25

## Question

Entscheidung aus [25](25-benachrichtigungsmodell.md) umsetzen:
- `public delegate Task AsyncEventHandler<in TEventArgs>(object sender, TEventArgs e);` und je Signal
  ein `event` mit `XxxEventArgs` statt `AsyncSignal<(...)>`: RemoteServer `StatusChanged`, `TimeUpdated`;
  BaseRemoteComponent `StatusChanged`, `NotificationReceived`; RemoteSkill `StateChanged`; RemoteVariable
  `ValueChanged`; RemoteLock `LockedChanged`, `LockingUserChanged`, `RenewDue`; RemoteUser `UserPresenceChanged`.
- Auslösen nacheinander, Ausnahme je Handler geloggt; nie auf dem SDK-Thread.
- `WatchStateAsync` (Skill), `WatchValueAsync` (Variable), `WatchStatusAsync` (Server) als
  `IAsyncEnumerable`: zuerst der aktuelle Wert, begrenzter Puffer je Aufrufer, älteste verwerfen.
- `AsyncSignal` löschen; `AutoLock`, Beispiele, Tests und README auf das neue Modell umstellen.
- Doku-Hinweis (XML-Doku am Delegate und README): In einem Handler nicht auf künftige
  Notifications desselben Clients warten (z. B. `StartAsync(StartWait.Completion)`), dafür Watch/WaitFor nutzen.
- Unit-Enum in PascalCase generieren (`tools/generate_units.py`), `Description` bleibt der Python-Name.
- `DEVIATIONS.md`: Signale als Abweichung von Python aufnehmen.

## Akzeptanz

- Unit: Ein Handler, der wirft, hält den nächsten Handler nicht auf; die Reihenfolge der Handler ist die Anmeldereihenfolge.
- Unit: `Watch…` liefert zuerst den aktuellen Wert; ein langsamer Konsument verliert alte Werte, bekommt aber den neuesten.
- Integration: `WatchStateAsync` sieht `Running` und `Completed` eines DummySkill-Laufs.
- Die volle Suite ist grün, 0 Warnungen.

## Answer

Umgesetzt (2026-10-29):
- `AsyncSignal` ist weg. Neu in `AsyncEvents.cs`: `AsyncEventHandler<TEventArgs>` (XML-Doku mit Hinweis auf die
  Re-Entrancy-Falle), je Signal ein `sealed record XxxEventArgs`, `RaiseAsync` (Handler nacheinander in
  Anmeldereihenfolge, Fehler je Handler geloggt) und `Watchers<T>` (je Aufrufer `Channel.CreateBounded(8, DropOldest)`,
  erst der aktuelle Wert, Abmelden bei Ende/Abbruch der Enumeration).
- Events: Server `StatusChanged`/`TimeUpdated`, Komponente `StatusChanged`/`NotificationReceived`, Skill `StateChanged`,
  Variable `ValueChanged`, Lock `LockedChanged`/`LockingUserChanged`/`RenewDue`, User `UserPresenceChanged`.
- Streams: `RemoteSkill.WatchStateAsync`, `RemoteVariable.WatchValueAsync` (`Variant`), `RemoteServer.WatchStatusAsync`.
- Statuswechsel aus SDK-Callbacks (KeepAlive, Reconnect) setzen `Status` sofort, die Handler laufen aber über
  `SubscriptionManager.Post` auf der Dispatch-Schleife statt auf dem SDK-Thread.
- `AutoLock`, Beispiele, Tests, README (Abschnitt „Änderungen beobachten“) und `DEVIATIONS.md` umgestellt.
- `Unit` in PascalCase aus `tools/generate_units.py` neu erzeugt; `EUInformation.Description` bleibt der Python-Name.
- Tests: `EventTests` (Reihenfolge/Isolation, aktueller Wert zuerst, Verlust alter Werte, Abmelden),
  `SkillTests.Watch_state_sees_running_and_completed`.
- Offen: `SPEC.md` beschreibt noch `AsyncSignal` (historisches Dokument, nicht angepasst). Watch-Streams enden nicht
  von selbst beim Disconnect, nur per Token.
