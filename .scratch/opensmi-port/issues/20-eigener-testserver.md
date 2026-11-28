# 20 Own test server per test and agent

Type: task
Status: resolved
Blocked by: 05

## Question

Security and reconnect need a server that the test can start encrypted, or stop and
restart. Agents working in parallel must not share locks or `obj/`.

## Answer

`tests/OpenSmi.Client.Tests/TestServer.cs`: `TestServer.StartAsync(port, encryption)` copies
`opensmi-server/examples/dummy` into a temp directory and patches the port and `encryption` in
`config.toml` there. There are `StopAsync`, `StartAgainAsync` and `CertificatePath` (the server certificate,
created on the first encrypted start). The shared server of the collection `server` runs
on `OPENSMI_TEST_PORT` (default 4843). Parallel agents build with
`--artifacts-path .artifacts/<name>` (gitignored). Test: `TestServerTests`.
