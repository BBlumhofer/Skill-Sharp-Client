# 24 Multi-target net8.0 and net10.0

Type: task
Status: resolved
Blocked by: 29

## Question

`OpenSmi.Client` builds for `net8.0;net10.0` (decision 03). The tests run against both targets.
Known pitfalls: `System.Threading.Lock` only exists from .NET 9 on (fall back to `object` for net8,
without `#if` if possible), `Lock` fields in `AsyncSignal`, `RemoteUaObject` and
`RemoteSkill`. SDK 8.0.423 is installed. The examples stay `net10.0`. Waits until the
running agents are done, because `Directory.Build.props` affects all builds.

## Acceptance

- `dotnet build` without warnings for both targets
- the suite is green for `net8.0` and `net10.0`

## Answer

With the user's consent, two missing packages were fetched once from nuget.org into the local
cache (`System.Formats.Asn1` 9.0.9, `System.Diagnostics.DiagnosticSource` 10.0.0); the
NuGet configuration is unchanged. `Directory.Build.props` now has `net8.0;net10.0`, the
examples stay `net10.0`. The example tests run only under net10.0, the lock fields are
`object`. The test targets run one after the other (`TestTfmsInParallel=false`) because they share the
dummy server and fixed ports. Result: net8.0 66/66, net10.0 78/78.
