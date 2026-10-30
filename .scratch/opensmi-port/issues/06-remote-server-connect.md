# 06 RemoteServer: Connect, Disconnect, Status, Retry

Type: task
Status: resolved
Blocked by: 05

## Question

`RemoteServerOptions`, `RemoteServer.ConnectAsync/DisconnectAsync/DisposeAsync`, `Status` plus
`StatusChanged` (feuert nur bei Änderung), `GetNamespaceIndex`, `Username`. Retry mit Backoff;
falsche Zugangsdaten führen sofort zu `OpenSmiAuthenticationException`. Orakel:
`remote_server.py:181-383`.

## Akzeptanz (Integration)

- Connect mit orchestrator → `Status == Connected`, Statusfolge Connecting → Browsing → Connected
- falsches Passwort → `OpenSmiAuthenticationException`
- nicht erreichbarer Port, `MaxRetries = 1` → `OpenSmiConnectionException`
- `GetNamespaceIndex("http://opcfoundation.org/UA/Machinery/")` = Index aus dem NamespaceArray

## Answer

`RemoteServer.cs`. Die Session entsteht direkt über `DefaultSessionFactory.Instance`, ohne Reflection und ohne `ApplicationConfiguration.Validate`, das ohne Zertifikatsstores scheitert. `shortcut:` NoSecurity bis Ticket 18. Tests: `RemoteServerTests` (Statusfolge, Auth-Fehler, Retry, Namespace).
