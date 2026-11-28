# 21 Security: SecurityOptions, persistent certificate, trust store

Type: task
Status: resolved
Blocked by: 20

## Question

Implement SPEC §7.3. `RemoteServerOptions.Security` of type `SecurityOptions(SecurityPolicyUri,
Mode, CertificateStorePath, AutoAcceptServerCertificate = false, TrustedPeerStorePath = null)` with
`SecurityOptions.None` as the default (current behavior). With security, a self-signed
client certificate is created once in the store (CN "OpenSMI C# Client", SAN = ApplicationUri) and
reused. Server certificates are checked against the trust store; without
`AutoAcceptServerCertificate` an unknown server fails. Oracle:
`remote_server.py:325-349` (Python creates a new one on every connect and checks nothing; both are an
intended deviation, SPEC §10).

## Acceptance (integration, `TestServer.StartAsync(port, encryption: true)`)

- `Basic256Sha256` + `SignAndEncrypt` with `AutoAcceptServerCertificate = true` → connected, browse returns `DummyMachine`
- a second connect with the same store → the same client certificate (same thumbprint)
- without AutoAccept and with an empty trust store → `OpenSmiConnectionException` (no endless retry loop)
- server certificate (`TestServer.CertificatePath`) in the trust store → connected without AutoAccept
- `SecurityOptions.None` against a server with `encryption = true` → `OpenSmiConnectionException`

## Answer

Built:
- `src/OpenSmi.Client/SecurityOptions.cs`: record as in SPEC §7.3, `SecurityOptions.None` (policy None, mode None) as the default in `RemoteServerOptions.Security`.
- `RemoteServer.CreateSessionAsync`: fetches the endpoints via `DiscoveryClient.GetEndpointsAsync` and takes exactly the one with the matching policy and mode; if there is none, `OpenSmiConnectionException` comes immediately (no retry). With security, directory stores are created under `CertificateStorePath` (`own`, `trusted`, `issuers`, `rejected`; `TrustedPeerStorePath` replaces `trusted`). The client certificate (CN "OpenSMI C# Client", SAN = ApplicationUri) is created by `ApplicationInstance.CheckApplicationInstanceCertificatesAsync` on the first connect and reused after that. `AutoAcceptServerCertificate` maps to `AutoAcceptUntrustedCertificates`.
- `ConnectAsync`: `BadCertificateUntrusted` is not retried; it becomes an `OpenSmiConnectionException` immediately.
- The `shortcut:` comment is gone.

Tests: `tests/OpenSmi.Client.Tests/SecurityTests.cs` with 5 integration tests against an encrypted `TestServer` on port 4862 (class fixture). They cover the five acceptance criteria. The thumbprint equality is checked through the file in `<store>/own/certs` because `ISession` does not expose its configuration.

Deviations:
- `CertificateStorePath = null` with security uses `%LOCALAPPDATA%/OpenSmi.Client/pki`.
- Endpoint selection is now exact (policy + mode) instead of `CoreClientUtils.SelectEndpointAsync`. This also applies to None.
- The entry in `docs/opensmi/DEVIATIONS.md` (SPEC §10) is missing because docs/ belongs to another agent.
