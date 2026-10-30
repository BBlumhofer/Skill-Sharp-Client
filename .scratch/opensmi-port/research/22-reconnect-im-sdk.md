# 22 Reconnect im OPC-Foundation-SDK 1.5.377.22

Quellen (Tag `1.5.377.22`, Basis-URL `https://github.com/OPCFoundation/UA-.NETStandard/blob/1.5.377.22/`):
- `Libraries/Opc.Ua.Client/Session/SessionReconnectHandler.cs` (kurz **SRH**)
- `Libraries/Opc.Ua.Client/Session/Session.cs` (kurz **S**)
- `Libraries/Opc.Ua.Client/Subscription/Subscription.cs` (kurz **Sub**), `.../Subscription/MonitoredItem.cs` (kurz **MI**)
- `Applications/ConsoleReferenceClient/UAClient.cs` (kurz **UAC**)
- XML-Doku: `~/.nuget/packages/opcfoundation.netstandard.opc.ua.client/1.5.377.22/lib/netstandard2.1/Opc.Ua.Client.xml`

Die Zeilennummern beziehen sich auf die Rohdateien bei diesem Tag. Gegen Server wurde nichts getestet.

## 1. Wann feuert `KeepAlive` mit Bad-Status, und wie startet man den Handler?

- Die Session schickt alle `KeepAliveInterval` ms einen Read auf den ServerStatus. `KeepAliveStopped` wird true, wenn seit dem letzten guten KeepAlive mehr als `KeepAliveInterval * 1 + 1000 ms` vergangen sind (Faktor 1, Guard-Band 1000 ms) oder ein anderer Fehler gemeldet wurde (S 634-650, 7180-7185).
- Im nächsten Timer-Tick ruft `OnSendKeepAlive` dann `OnKeepAliveError(BadNoCommunication)` auf (S 4451-4458). Das löst das `KeepAlive`-Event mit `e.Status` = Bad und `ServerState.Unknown` aus (S 4600-4630). Auch fehlschlagende KeepAlive-Reads landen dort. Wie der Read-Fehler im Detail verarbeitet wird, habe ich nicht geprüft (unverifiziert).
- Während die Session reconnectet (`m_reconnecting`), werden KeepAlives übersprungen (S 4440-4446).
- Gestartet wird der Handler wie im Sample (UAC 349-396): im Handler `if (ServiceResult.IsBad(e.Status)) { handler.BeginReconnect(Session, reconnectPeriod, callback); e.CancelKeepAlive = true; }`. Ruft man ihn im Zustand `Triggered` oder `Reconnecting` erneut auf, startet nichts Neues. Höchstens wird die Periode verkürzt (SRH 238-262). Events von verworfenen Sessions filtert das Sample mit `Session.Equals(session)` heraus (UAC 353).
- Konstruktor: `new SessionReconnectHandler(reconnectAbort: true, maxReconnectPeriod)`. Mit `reconnectAbort=true` bricht der Handler ab, wenn sich der KeepAlive inzwischen von selbst erholt hat (SRH 330-343).

## 2. Dieselbe Session oder eine neue? Was passiert mit Subscriptions und MonitoredItems?

Das hängt vom Pfad in `DoReconnectAsync` ab (SRH 401-584):

- **Reactivate** (der Server lebt noch, die Session ist nicht abgelaufen): `Session.ReconnectAsync()` läuft auf **demselben** Objekt. Subscriptions und MonitoredItems bleiben dieselben Instanzen (Kommentar "monitored items should start updating on their own", SRH 419-426). `handler.Session` ist danach dieselbe Referenz (UAC 418-436: "after reactivate, the same session instance may be returned").
- **KeepAlive erholt** (`reconnectAbort`): `handler.Session == null`, es ändert sich nichts (SRH 338-343, UAC 438-442).
- **Recreate**: `SessionFactory.RecreateAsync(template, ...)` liefert ein **neues `Session`-Objekt** (SRH 520-548, Kommentar: "the template session is not connected at this point and must be disposed by the owner"). `CloneSession(channel, true)` kopiert die Session-Events (`KeepAlive`, `Publish` usw., S 136-148). Jede Subscription wird über `CloneSubscription(copyEventHandlers)` **geklont** (S 150-153). Der Klon übernimmt `Handle`, `TransferId` und `DisplayName`. Jedes MonitoredItem wird mit `CloneMonitoredItem(copyEventHandlers, copyClientHandle: true)` geklont (Sub 72-117), inklusive `Notification`-Event (MI 132-134). Die **Instanzen sind also neu**, die Handler-Delegates hängen aber weiter dran.
- Mit `TransferSubscriptionsOnReconnect = true` (Default `false`, S 586-594) versucht `RecreateSubscriptionsAsync` zuerst den Transfer-Service. Schlägt er fehl, legt `CreateAsync` die Subscriptions neu an. Bei `BadServiceUnsupported` schaltet das SDK das Flag selbst ab (S 4131-4180). Das Sample setzt `TransferSubscriptionsOnReconnect = true` und `DeleteSubscriptionsOnClose = false` (UAC 273-275).
- Unverifiziert: an welche Objekte genau `TransferAsync` geht (Template- oder Klon-Liste, S 3723 und 2232ff.). Für uns zählt nur, dass die neue Session danach die Klone hält.

## 3. Server-Neustart

- `ReconnectAsync` schlägt fehl. Bei Kommunikationsfehlern wartet der Handler weiter, solange die geschätzte SessionTimeout-Zeit noch nicht abgelaufen ist (SRH 436-457). Bei `BadSessionIdInvalid` (die Session ist auf dem Server weg) startet sofort ein Recreate über den bestehenden Kanal (SRH 469-474). Bei allen anderen Fehlern setzt der Handler `m_reconnectFailed`, und der nächste Versuch ist ein Recreate.
- Das Recreate öffnet eine neue Session. Der Transfer scheitert, und das SDK legt **alle geklonten Subscriptions samt MonitoredItems selbst neu an**: `CreateAsync` für jede Subscription mit `!Created` (S 4170-4178, Kanal-Variante S 3830-3836). Der Client muss nichts selbst anlegen, und die neuen Instanzen tragen die kopierten Handler.
- Schlägt auch das Recreate fehl, weil der Server noch nicht wieder läuft, plant der Handler den nächsten Versuch mit Backoff ein (SRH 370-394, 549-583). Der Callback kommt erst bei Erfolg.
- Ob NamespaceIndex und NodeIds nach einem Neustart stabil bleiben, entscheidet der Server. Das ist ungeprüft.

## 4. Perioden und Backoff

- Konstanten: `MinReconnectPeriod` 500 ms, `DefaultReconnectPeriod` 1000 ms, **`MaxReconnectPeriod` 30000 ms als harte Obergrenze** (SRH 45-55, XML-Doku Z. 3371-3385).
- Exponentielles Backoff gibt es nur, wenn `maxReconnectPeriod > 500` übergeben wird. Dann verdoppelt sich die Periode pro Fehlversuch bis `maxReconnectPeriod`, begrenzt auf [500, 30000] (SRH 108-113, 287-302). Dazu kommt ±10 % Jitter (SRH 271-281). Mit `-1` (Default) gibt es kein Backoff.
- Das Sample verwendet `ReconnectPeriod = 1000`, `ReconnectPeriodExponentialBackoff = 15000` und `KeepAliveInterval = 5000` (UAC 101-111).
- Pythons `reconnect_max_delay = 30 s` deckt sich mit der SDK-Obergrenze von 30000 ms.

## Empfehlung für Ticket 23

1. **`RemoteServer` besitzt den `SessionReconnectHandler`** (`reconnectAbort: true`, `maxReconnectPeriod: ReconnectMaxDelay`, Default 30 s, mehr erlaubt das SDK ohnehin nicht). Im `KeepAlive`-Handler bei `IsBad(e.Status)`: `BeginReconnect(Session, 1000, OnReconnectComplete)` aufrufen, `e.CancelKeepAlive = true` setzen und den Status auf `Reconnecting` stellen. Events fremder Sessions ignorieren.
2. **`RemoteServer.Session` wird ausgetauscht, nicht festgehalten.** Im Callback:
   - `handler.Session == null` oder dieselbe Referenz: nichts tun.
   - neue Referenz: `Session = handler.Session` setzen und die alte Session nur mit `Dispose()` freigeben. Kein `CloseAsync`, sonst löscht der Server die Subscriptions.

   Danach den Status auf `Connected` setzen. Der KeepAlive-Handler wird mitkopiert und darf nicht noch einmal angehängt werden. Alle Aufrufer holen die Session weiter über `RequireSession()` und halten keine eigene Referenz.
3. Beim Erzeugen der Session `TransferSubscriptionsOnReconnect = true` und `DeleteSubscriptionsOnClose = false` setzen. In `DisconnectAsync` zuerst `handler.CancelReconnect()`/`Dispose()` aufrufen, dann `CloseAsync(deleteSubscriptions: true)`. Das gilt auch, wenn gerade ein Reconnect läuft.
4. **`SubscriptionManager`**: Das Feld `readonly Subscription _subscription` zeigt nach einem Recreate auf ein totes Objekt. Deshalb braucht es eine Methode `Rebind(ISession)`, die die neue Subscription in `newSession.Subscriptions` sucht (über `Handle`, das mitkopiert wird, Sub 91, oder als einzige Subscription) und `_subscription` ersetzt. Die Handler-Dictionaries nach NodeId und die Notification-Lambdas bleiben gültig, weil die Delegates mitgeklont werden. Die Domain-Objekte (Machines usw.) bleiben unverändert, ein neuer Browse ist nicht nötig.
5. **Server-Neustart**: Eigene Logik ist nicht nötig, das SDK legt die Subscription neu an. Danach MonitoredItems mit Bad-Status loggen (wie in `ApplyChangesAsync`). Der Heartbeat-Rebuild (`HeartbeatStaleAfter`) fängt den Fall ab, dass trotzdem keine Notifications kommen.
6. **Perioden**: `KeepAliveInterval` 5000 ms, damit wird ein Abbruch nach etwa 6 bis 11 s erkannt. Für die 30-s-Akzeptanz eventuell 2000 ms nehmen. Start-Periode 1000 ms, `maxReconnectPeriod` = `ReconnectMaxDelay` (Default 30 s).
