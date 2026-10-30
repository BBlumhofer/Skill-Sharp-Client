# 22 Reconnect im OPC-Foundation-SDK 1.5.377

Type: research
Status: resolved
Blocked by: –

## Question

Wie verhält sich `OPCFoundation.NetStandard.Opc.Ua.Client` 1.5.377.22 beim Verbindungsverlust,
wenn **wir** den Reconnect über `SessionReconnectHandler` steuern?

1. Wann feuert `ISession.KeepAlive` mit Bad-Status, und wie startet man den Handler (`BeginReconnect`)?
2. Bleibt nach erfolgreichem Reconnect dasselbe `ISession`-Objekt erhalten, oder liefert der Handler
   eine neue Session (`ReconnectHandler.Session`)? Was passiert mit `Subscription`/`MonitoredItem`-Objekten
   und ihren `Notification`-Handlern: werden sie per Transfer übernommen
   (`TransferSubscriptionsOnReconnect`) oder neu angelegt?
3. Was passiert, wenn der **Server neu startet** (Session ist weg, Transfer scheitert)? Werden die
   Subscriptions neu erzeugt, oder muss der Client sie selbst neu anlegen?
4. Welche Defaults (Reconnect-Periode, Backoff `maxReconnectPeriod`) sind sinnvoll gegenüber Python
   (`reconnect_max_delay` 30 s)?

Quellen: XML-Doku im NuGet-Cache
(`~/.nuget/packages/opcfoundation.netstandard.opc.ua.client/1.5.377.22`), das GitHub-Repo
OPCFoundation/UA-.NETStandard beim Tag 1.5.377.22 (`SessionReconnectHandler.cs`, `Session.cs`,
Sample `ConsoleReferenceClient/UAClient.cs`).

## Answer

- `KeepAlive` meldet Bad-Status, sobald ein KeepAlive länger als `KeepAliveInterval`+1 s ausbleibt. Im Handler `BeginReconnect` aufrufen und `CancelKeepAlive` setzen, wie im ConsoleReferenceClient.
- Reactivate behält dieselbe Session samt allen Subscriptions und Items. Ein Recreate (auch nach einem Server-Neustart) liefert eine neue `Session` mit geklonten Subscriptions und MonitoredItems, kopiert die `Notification`-Handler mit und legt die Subscriptions auf dem Server selbst neu an.
- Ticket 23: `RemoteServer.Session` durch `handler.Session` ersetzen, die alte Session nur disposen, `SubscriptionManager` per `Rebind` auf die geklonte Subscription umhängen (Handle). Die Domain-Objekte bleiben unverändert.
- Backoff: Start 1000 ms, `maxReconnectPeriod` = 30 s (SDK-Maximum, entspricht Python).

[Details](../research/22-reconnect-im-sdk.md)
