# 23 Reconnect mit erhaltenen Objektidentitäten und Heartbeat-Rebuild

Type: task
Status: resolved
Blocked by: 21, 22

## Question

SPEC §4.1 und §7.1: Es gibt nur **einen** Reconnect-Mechanismus über den `SessionReconnectHandler`.
Der Status wechselt `Connected → Reconnecting → Connected`. Nach dem Reconnect sind `Machines` und
alle Objekte **dieselben Instanzen** (kein neuer Browse), und Notifications kommen weiter an. Der
Heartbeat über `CurrentTime` baut die Subscription mit allen Data- **und** Event-Items neu auf,
wenn länger als `HeartbeatStaleAfter` (Default 10 s) nichts kam. `AutoReconnect` und
`ReconnectMaxDelay` kommen als Options dazu. Wie das SDK sich verhält, steht in Ticket 22.

## Akzeptanz (Integration, eigener `TestServer`)

- Server stoppen → Status `Reconnecting`; Server neu starten → Status `Connected` innerhalb von 30 s
- danach `ReferenceEquals` für die Maschine vor und nach dem Reconnect, und `ValueChanged` eines
  Monitoring-Variablen feuert wieder
- `DisconnectAsync` während `Reconnecting` beendet sauber mit `Disconnected`

## Answer

**Design** (wie Ticket 22 empfiehlt):
- `RemoteServerOptions`: `AutoReconnect` (true), `ReconnectMaxDelay` (30 s), `HeartbeatStaleAfter` (10 s).
- `RemoteServer` besitzt einen `SessionReconnectHandler(reconnectAbort: true, ReconnectMaxDelay)`.
  `KeepAliveInterval` = 2000 ms. Bei Bad-KeepAlive der aktuellen Session: `BeginReconnect(…, 1000)`,
  `CancelKeepAlive`, Status `Reconnecting`. Im Callback wird bei neuer Session `Session` getauscht, die
  alte nur `Dispose()`t, und `SubscriptionManager.Rebind` sucht die geklonte Subscription über `Handle`.
  Danach `Connected`. Objekte bleiben dieselben, kein neuer Browse.
- `DisconnectAsync`/`TearDown` disposed zuerst den Handler; ein Callback nach dem Disconnect wird
  ignoriert und eine dabei entstandene Session freigegeben.
- `SubscriptionManager`: Jede Notification (Data und Event) setzt den Heartbeat-Zeitstempel. Eine
  Schleife prüft alle `HeartbeatStaleAfter/4`; nur im Status `Connected` zählt die Zeit. Bei Stale wird
  die Subscription gelöscht (silent) und aus der Session entfernt, dann neu mit allen Data- und
  Event-Items angelegt. Schlägt das fehl, gibt es beim nächsten Tick einen neuen Versuch. Ein
  `SemaphoreSlim` serialisiert Subscribe und Rebuild. Den `shortcut:`-Kommentar zum Rebuild habe ich
  entfernt, der zu Unsubscribe bleibt.

**Tests** (`ReconnectTests`, eigene `TestServer` auf 4894–4896): Neustart → `Reconnecting` →
`Connected`, dieselbe Maschine, `ValueChanged` feuert wieder. `DisconnectAsync` während `Reconnecting`
endet in `Disconnected` und bleibt dort, auch wenn der Server wiederkommt. Heartbeat: Die Subscription
wird über die rohe Session serverseitig gelöscht, danach entsteht eine neue Subscription mit gleicher
Item-Zahl, und `ValueChanged` feuert wieder.

**Abweichungen/Risiken:**
- Nicht gesetzt: `TransferSubscriptionsOnReconnect`/`DeleteSubscriptionsOnClose`. Der Neustart braucht
  sie nicht, weil das SDK die Subscription selbst neu anlegt.
- Bei `AutoReconnect = false` reagiert der Client nicht auf einen toten Server, der Status bleibt
  `Connected`. Python würde hier auf `Disconnected` wechseln. Das ist nicht gebaut.
- `KeepAliveInterval` ist fest auf 2 s eingestellt. `WatchdogInterval` aus SPEC §4.1 ist nicht verdrahtet.
- Einzel-Item-Retry mit Backoff (SPEC §7.1) ist nicht gebaut, fehlgeschlagene Items werden nur geloggt.
- Der Reactivate-Pfad (dieselbe Session) ist nicht getestet, nur der Recreate-Pfad über einen Server-Neustart.
