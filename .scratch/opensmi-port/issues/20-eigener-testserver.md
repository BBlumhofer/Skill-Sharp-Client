# 20 Eigener Testserver je Test und Agent

Type: task
Status: resolved
Blocked by: 05

## Question

Security und Reconnect brauchen einen Server, den der Test verschlüsselt starten bzw. stoppen und
neu starten kann. Parallel arbeitende Agenten dürfen sich weder Locks noch `obj/` teilen.

## Answer

`tests/OpenSmi.Client.Tests/TestServer.cs`: `TestServer.StartAsync(port, encryption)` kopiert
`opensmi-server/examples/dummy` in ein Temp-Verzeichnis und patcht dort Port und `encryption` in
`config.toml`. Es gibt `StopAsync`, `StartAgainAsync` und `CertificatePath` (Server-Zertifikat,
wird beim ersten verschlüsselten Start erzeugt). Der gemeinsame Server der Collection `server` läuft
auf `OPENSMI_TEST_PORT` (Default 4843). Parallele Agenten bauen mit
`--artifacts-path .artifacts/<name>` (gitignored). Test: `TestServerTests`.
