# 21 Security: SecurityOptions, persistentes Zertifikat, Trust-Store

Type: task
Status: resolved
Blocked by: 20

## Question

SPEC §7.3 umsetzen. `RemoteServerOptions.Security` vom Typ `SecurityOptions(SecurityPolicyUri,
Mode, CertificateStorePath, AutoAcceptServerCertificate = false, TrustedPeerStorePath = null)` mit
`SecurityOptions.None` als Default (heutiges Verhalten). Mit Security wird ein selbstsigniertes
Client-Zertifikat **einmal** im Store erzeugt (CN „OpenSMI C# Client“, SAN = ApplicationUri) und
wiederverwendet. Server-Zertifikate werden gegen den Trust-Store geprüft; ohne
`AutoAcceptServerCertificate` scheitert ein unbekannter Server. Orakel:
`remote_server.py:325-349` (Python erzeugt pro Connect neu und prüft nichts; beides ist eine
gewollte Abweichung, SPEC §10).

## Akzeptanz (Integration, `TestServer.StartAsync(port, encryption: true)`)

- `Basic256Sha256` + `SignAndEncrypt` mit `AutoAcceptServerCertificate = true` → verbunden, Browse liefert `DummyMachine`
- zweiter Connect mit demselben Store → dasselbe Client-Zertifikat (Thumbprint gleich)
- ohne AutoAccept und mit leerem Trust-Store → `OpenSmiConnectionException` (kein Retry-Endlosloop)
- Server-Zertifikat (`TestServer.CertificatePath`) im Trust-Store → verbunden ohne AutoAccept
- `SecurityOptions.None` gegen einen Server mit `encryption = true` → `OpenSmiConnectionException`

## Answer

Gebaut:
- `src/OpenSmi.Client/SecurityOptions.cs`: Record laut SPEC §7.3, `SecurityOptions.None` (Policy None, Mode None) als Default in `RemoteServerOptions.Security`.
- `RemoteServer.CreateSessionAsync`: holt die Endpoints per `DiscoveryClient.GetEndpointsAsync` und nimmt genau den mit passender Policy und Mode; gibt es keinen, kommt sofort `OpenSmiConnectionException` (kein Retry). Mit Security werden Directory-Stores unter `CertificateStorePath` angelegt (`own`, `trusted`, `issuers`, `rejected`; `TrustedPeerStorePath` ersetzt `trusted`). Das Client-Zertifikat (CN „OpenSMI C# Client“, SAN = ApplicationUri) erzeugt `ApplicationInstance.CheckApplicationInstanceCertificatesAsync` beim ersten Connect und nutzt es danach wieder. `AutoAcceptServerCertificate` geht auf `AutoAcceptUntrustedCertificates`.
- `ConnectAsync`: `BadCertificateUntrusted` wird nicht wiederholt, sondern sofort zur `OpenSmiConnectionException`.
- Der `shortcut:`-Kommentar ist weg.

Tests: `tests/OpenSmi.Client.Tests/SecurityTests.cs` mit 5 Integrationstests gegen einen verschlüsselten `TestServer` auf Port 4862 (Klassen-Fixture). Sie decken die fünf Akzeptanzkriterien ab. Die Thumbprint-Gleichheit wird über die Datei in `<store>/own/certs` geprüft, weil `ISession` keine Konfiguration herausgibt.

Abweichungen:
- `CertificateStorePath = null` mit Security nimmt `%LOCALAPPDATA%/OpenSmi.Client/pki`.
- Die Endpoint-Auswahl ist jetzt exakt (Policy + Mode) statt `CoreClientUtils.SelectEndpointAsync`. Das gilt auch für None.
- Der Eintrag in `docs/opensmi/DEVIATIONS.md` (SPEC §10) fehlt, weil docs/ einem anderen Agenten gehört.
