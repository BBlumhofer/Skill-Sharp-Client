# Connecting

`RemoteServer` is the entry point. Create it with `RemoteServerOptions`, call `ConnectAsync`, and dispose it with
`await using` to disconnect.

```csharp
await using var server = await new RemoteServer(new()
{
    Url = "opc.tcp://localhost:4843/server", Username = "orchestrator", Password = "orchestrator",
}).ConnectAsync();
```

`ConnectAsync` opens the session, browses the server (see [browsing](browsing.md)) and subscribes to everything it
found. When it returns, machines, users, variable values and skill and component states are known.

## Options

| Option | Default | Meaning |
|---|---|---|
| `Url`, `Username`, `Password` | required | endpoint and login |
| `BrowseRules` | `null` (browse everything) | an empty list browses nothing |
| `Security` | `SecurityOptions.None` | see below |
| `RequestTimeout` | 10 s | timeout of a single OPC UA request |
| `PublishingInterval` | 100 ms | interval of the subscription |
| `WatchdogInterval` | 1 s | session keep-alive; a dead server is noticed after about this plus one second |
| `AutoReconnect` | `true` | reconnect after a lost connection |
| `ReconnectMaxDelay` | 30 s | upper bound of the reconnect backoff |
| `HeartbeatStaleAfter` | 10 s | without any notification for this long, the subscription is rebuilt |
| `LoggerFactory` | none | `ILoggerFactory` for the client's logs |

`ConnectAsync(new ConnectRetryOptions(MaxRetries: 3, MinDelaySeconds: 0.5, MaxDelaySeconds: 30))` sets the retry
with exponential backoff; `MaxRetries: null` retries forever. A rejected login throws
`OpenSmiAuthenticationException`, an untrusted server certificate `OpenSmiConnectionException` without retrying.

## Status

`Status` is one of `Disconnected`, `Connecting`, `Browsing`, `Connected`, `Reconnecting` and `Disconnecting`.
`StatusChanged` and `WatchStatusAsync` report every change. `TimeUpdated` reports the server's current time as it
ticks, which makes it a heartbeat.

After a lost connection the client goes to `Reconnecting`, recreates the session and keeps all objects: a
`RemoteSkill` you hold stays the same object and receives updates again. Without `AutoReconnect`, a lost connection
ends in `Disconnected`.

`DisconnectAsync` can be called several times and concurrently; all calls wait for the same teardown.

## Security

```csharp
Security = new SecurityOptions(SecurityPolicies.Basic256Sha256, MessageSecurityMode.SignAndEncrypt, "pki"),
```

The client certificate is created once in `pki/own` and reused. The server certificate must be in `pki/trusted/certs`
(or in `TrustedPeerStorePath`), otherwise the connection is rejected. `AutoAcceptServerCertificate: true` turns the
check off and is meant for tests only. Without a store path, the store is in the local application data folder under
`OpenSmi.Client/pki`.

## Raw access

`Session` is the OPC Foundation `ISession` for anything the client does not cover, `GetUaObject(nodeId)` returns
the object browsed for a node and `GetNamespaceIndex(uri)` the index of a namespace.
