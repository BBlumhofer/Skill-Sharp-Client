# 19 Examples, README, DEVIATIONS.md

Type: task
Status: resolved
Blocked by: 17, 20

## Question

Port the Python examples from `opensmi-client/examples/` (reference checkout, see the map) to `examples/`,
in one console project `examples/OpenSmi.Client.Examples` with the example name as the
first argument (`dotnet run -- quick_start`). Add a README in the repo root (replacing the
UTF-16 file) and `docs/opensmi/DEVIATIONS.md` with every deviation from SPEC §10, its state in the code
and the test that proves it (if a test is missing, add one). `security_policy` follows only with
ticket 21; until then the README lists it as "to follow".

## Acceptance

- every example runs against a `TestServer` without an exception; an integration test calls the
  example entry points directly (no processes)
- `DEVIATIONS.md` refers to an existing test name for every row

## Answer

All 12 examples live in `examples/OpenSmi.Client.Examples`, each as a class `<Name>Example` with
`RunAsync(url, …, CancellationToken)`; `Program.cs` picks one by the first argument. `security_policy` was
added after all, following ticket 21 (Basic256Sha256 instead of Python's Aes256Sha256RsaPss, as the coordinator
specified). Examples that run forever or for 120 s in Python take an optional `duration`.
`quick_start` takes the machine and skill names as parameters because the test uses the dummy server instead of the
quick_start server. `example.py` is called `ColdStartExample` in C#.

Tests: `ExamplesTests` (11 examples, own server on port 4872), `SecurityPolicyExampleTests`
(encrypted server on 4873), `DeviationTests` (timeout/cancel while waiting, the only
deviation without a test). `DEVIATIONS.md` has 11 rows (the security row from SPEC §10 is split into
certificate and trust store), all implemented and with an existing test name. The UTF-16 README
has been replaced.
