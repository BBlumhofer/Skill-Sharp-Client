# 06 RemoteServer: connect, disconnect, status, retry

Type: task
Status: resolved
Blocked by: 05

## Question

`RemoteServerOptions`, `RemoteServer.ConnectAsync/DisconnectAsync/DisposeAsync`, `Status` plus
`StatusChanged` (fires only on a change), `GetNamespaceIndex`, `Username`. Retry with backoff;
wrong credentials lead straight to `OpenSmiAuthenticationException`. Oracle:
`remote_server.py:181-383`.

## Acceptance (integration)

- connect as orchestrator → `Status == Connected`, status sequence Connecting → Browsing → Connected
- wrong password → `OpenSmiAuthenticationException`
- unreachable port, `MaxRetries = 1` → `OpenSmiConnectionException`
- `GetNamespaceIndex("http://opcfoundation.org/UA/Machinery/")` = index from the NamespaceArray

## Answer

`RemoteServer.cs`. The session is created directly through `DefaultSessionFactory.Instance`, without reflection and without `ApplicationConfiguration.Validate`, which fails without certificate stores. `shortcut:` NoSecurity until ticket 18. Tests: `RemoteServerTests` (status sequence, auth error, retry, namespace).
