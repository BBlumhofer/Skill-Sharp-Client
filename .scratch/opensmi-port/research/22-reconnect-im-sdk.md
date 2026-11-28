# 22 Reconnect in the OPC Foundation SDK 1.5.377.22

Sources (tag `1.5.377.22`, base URL `https://github.com/OPCFoundation/UA-.NETStandard/blob/1.5.377.22/`):
- `Libraries/Opc.Ua.Client/Session/SessionReconnectHandler.cs` (short **SRH**)
- `Libraries/Opc.Ua.Client/Session/Session.cs` (short **S**)
- `Libraries/Opc.Ua.Client/Subscription/Subscription.cs` (short **Sub**), `.../Subscription/MonitoredItem.cs` (short **MI**)
- `Applications/ConsoleReferenceClient/UAClient.cs` (short **UAC**)
- XML docs: `~/.nuget/packages/opcfoundation.netstandard.opc.ua.client/1.5.377.22/lib/netstandard2.1/Opc.Ua.Client.xml`

The line numbers refer to the raw files at this tag. Nothing was tested against a server.

## 1. When does `KeepAlive` fire with a bad status, and how do you start the handler?

- The session sends a read on the ServerStatus every `KeepAliveInterval` ms. `KeepAliveStopped` becomes true when more than `KeepAliveInterval * 1 + 1000 ms` have passed since the last good KeepAlive (factor 1, guard band 1000 ms) or another error was reported (S 634-650, 7180-7185).
- On the next timer tick, `OnSendKeepAlive` then calls `OnKeepAliveError(BadNoCommunication)` (S 4451-4458). That raises the `KeepAlive` event with `e.Status` = Bad and `ServerState.Unknown` (S 4600-4630). Failing KeepAlive reads end up there as well. I did not check how the read error is processed in detail (unverified).
- While the session is reconnecting (`m_reconnecting`), KeepAlives are skipped (S 4440-4446).
- The handler is started as in the sample (UAC 349-396): in the handler, `if (ServiceResult.IsBad(e.Status)) { handler.BeginReconnect(Session, reconnectPeriod, callback); e.CancelKeepAlive = true; }`. Calling it again in state `Triggered` or `Reconnecting` starts nothing new. At most the period is shortened (SRH 238-262). The sample filters out events from discarded sessions with `Session.Equals(session)` (UAC 353).
- Constructor: `new SessionReconnectHandler(reconnectAbort: true, maxReconnectPeriod)`. With `reconnectAbort=true` the handler aborts if the KeepAlive has recovered by itself in the meantime (SRH 330-343).

## 2. The same session or a new one? What happens to subscriptions and MonitoredItems?

That depends on the path in `DoReconnectAsync` (SRH 401-584):

- Reactivate (the server is still alive, the session has not expired): `Session.ReconnectAsync()` runs on the same object. Subscriptions and MonitoredItems stay the same instances (comment "monitored items should start updating on their own", SRH 419-426). `handler.Session` is the same reference afterwards (UAC 418-436: "after reactivate, the same session instance may be returned").
- KeepAlive recovered (`reconnectAbort`): `handler.Session == null`, nothing changes (SRH 338-343, UAC 438-442).
- Recreate: `SessionFactory.RecreateAsync(template, ...)` returns a new `Session` object (SRH 520-548, comment: "the template session is not connected at this point and must be disposed by the owner"). `CloneSession(channel, true)` copies the session events (`KeepAlive`, `Publish` etc., S 136-148). Every subscription is cloned through `CloneSubscription(copyEventHandlers)` (S 150-153). The clone takes over `Handle`, `TransferId` and `DisplayName`. Every MonitoredItem is cloned with `CloneMonitoredItem(copyEventHandlers, copyClientHandle: true)` (Sub 72-117), including the `Notification` event (MI 132-134). So the instances are new, but the handler delegates stay attached.
- With `TransferSubscriptionsOnReconnect = true` (default `false`, S 586-594), `RecreateSubscriptionsAsync` first tries the transfer service. If it fails, `CreateAsync` creates the subscriptions anew. On `BadServiceUnsupported` the SDK turns the flag off by itself (S 4131-4180). The sample sets `TransferSubscriptionsOnReconnect = true` and `DeleteSubscriptionsOnClose = false` (UAC 273-275).
- Unverified: which objects exactly `TransferAsync` is applied to (template or clone list, S 3723 and 2232ff.). What matters for us is only that the new session holds the clones afterwards.

## 3. Server restart

- `ReconnectAsync` fails. On communication errors the handler keeps waiting as long as the estimated SessionTimeout has not expired (SRH 436-457). On `BadSessionIdInvalid` (the session is gone on the server) a recreate over the existing channel starts immediately (SRH 469-474). On all other errors the handler sets `m_reconnectFailed`, and the next attempt is a recreate.
- The recreate opens a new session. The transfer fails, and the SDK creates all cloned subscriptions together with their MonitoredItems by itself: `CreateAsync` for every subscription with `!Created` (S 4170-4178, channel variant S 3830-3836). The client does not have to create anything itself, and the new instances carry the copied handlers.
- If the recreate also fails because the server is not running again yet, the handler schedules the next attempt with backoff (SRH 370-394, 549-583). The callback comes only on success.
- Whether the NamespaceIndex and NodeIds stay stable after a restart is up to the server. This is unchecked.

## 4. Periods and backoff

- Constants: `MinReconnectPeriod` 500 ms, `DefaultReconnectPeriod` 1000 ms, `MaxReconnectPeriod` 30000 ms as a hard upper limit (SRH 45-55, XML docs l. 3371-3385).
- Exponential backoff only exists when `maxReconnectPeriod > 500` is passed. Then the period doubles per failed attempt up to `maxReconnectPeriod`, clamped to [500, 30000] (SRH 108-113, 287-302). On top of that there is ±10 % jitter (SRH 271-281). With `-1` (default) there is no backoff.
- The sample uses `ReconnectPeriod = 1000`, `ReconnectPeriodExponentialBackoff = 15000` and `KeepAliveInterval = 5000` (UAC 101-111).
- Python's `reconnect_max_delay = 30 s` matches the SDK upper limit of 30000 ms.

## Recommendation for ticket 23

1. `RemoteServer` owns the `SessionReconnectHandler` (`reconnectAbort: true`, `maxReconnectPeriod: ReconnectMaxDelay`, default 30 s; the SDK does not allow more anyway). In the `KeepAlive` handler on `IsBad(e.Status)`: call `BeginReconnect(Session, 1000, OnReconnectComplete)`, set `e.CancelKeepAlive = true` and set the status to `Reconnecting`. Ignore events of other sessions.
2. `RemoteServer.Session` is replaced, not held on to. In the callback:
   - `handler.Session == null` or the same reference: do nothing.
   - a new reference: set `Session = handler.Session` and release the old session only with `Dispose()`. No `CloseAsync`, otherwise the server deletes the subscriptions.

   Then set the status to `Connected`. The KeepAlive handler is copied along and must not be attached again. All callers keep getting the session through `RequireSession()` and hold no reference of their own.
3. When creating the session, set `TransferSubscriptionsOnReconnect = true` and `DeleteSubscriptionsOnClose = false`. In `DisconnectAsync`, call `handler.CancelReconnect()`/`Dispose()` first, then `CloseAsync(deleteSubscriptions: true)`. This also applies while a reconnect is running.
4. `SubscriptionManager`: the field `readonly Subscription _subscription` points to a dead object after a recreate. It therefore needs a method `Rebind(ISession)` that looks up the new subscription in `newSession.Subscriptions` (via `Handle`, which is copied along, Sub 91, or as the only subscription) and replaces `_subscription`. The handler dictionaries by NodeId and the notification lambdas stay valid because the delegates are cloned along. The domain objects (Machines etc.) stay unchanged; a new browse is not needed.
5. Server restart: no logic of our own is needed, the SDK recreates the subscription. Afterwards, log MonitoredItems with a bad status (as in `ApplyChangesAsync`). The heartbeat rebuild (`HeartbeatStaleAfter`) covers the case where notifications still do not arrive.
6. Periods: `KeepAliveInterval` 5000 ms, which detects a drop after about 6 to 11 s. For the 30 s acceptance, possibly use 2000 ms. Start period 1000 ms, `maxReconnectPeriod` = `ReconnectMaxDelay` (default 30 s).
