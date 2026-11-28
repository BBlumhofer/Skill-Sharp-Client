# 22 Reconnect in the OPC Foundation SDK 1.5.377

Type: research
Status: resolved
Blocked by: none

## Question

How does `OPCFoundation.NetStandard.Opc.Ua.Client` 1.5.377.22 behave on a connection loss
when we drive the reconnect ourselves through `SessionReconnectHandler`?

1. When does `ISession.KeepAlive` fire with a bad status, and how do you start the handler (`BeginReconnect`)?
2. After a successful reconnect, is the same `ISession` object kept, or does the handler return
   a new session (`ReconnectHandler.Session`)? What happens to `Subscription`/`MonitoredItem` objects
   and their `Notification` handlers: are they taken over by transfer
   (`TransferSubscriptionsOnReconnect`) or created anew?
3. What happens when the server restarts (the session is gone, the transfer fails)? Are the
   subscriptions recreated, or does the client have to create them itself?
4. Which defaults (reconnect period, backoff `maxReconnectPeriod`) make sense compared to Python
   (`reconnect_max_delay` 30 s)?

Sources: XML docs in the NuGet cache
(`~/.nuget/packages/opcfoundation.netstandard.opc.ua.client/1.5.377.22`), the GitHub repo
OPCFoundation/UA-.NETStandard at tag 1.5.377.22 (`SessionReconnectHandler.cs`, `Session.cs`,
sample `ConsoleReferenceClient/UAClient.cs`).

## Answer

- `KeepAlive` reports a bad status as soon as a KeepAlive is missing for longer than `KeepAliveInterval`+1 s. In the handler, call `BeginReconnect` and set `CancelKeepAlive`, as in the ConsoleReferenceClient.
- Reactivate keeps the same session with all its subscriptions and items. A recreate (also after a server restart) returns a new `Session` with cloned subscriptions and MonitoredItems, copies the `Notification` handlers along and creates the subscriptions on the server by itself.
- Ticket 23: replace `RemoteServer.Session` with `handler.Session`, only dispose the old session, rebind `SubscriptionManager` to the cloned subscription via `Rebind` (handle). The domain objects stay unchanged.
- Backoff: start at 1000 ms, `maxReconnectPeriod` = 30 s (SDK maximum, matches Python).

[Details](../research/22-reconnect-im-sdk.md)
