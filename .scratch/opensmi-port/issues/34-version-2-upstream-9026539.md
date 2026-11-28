# 34 Version 2.0.0: upstream 9026539, opensmi-server 0.3.1, SkillSharp replaced

Type: task
Status: done (2026-11-27)

## Question

Bring the client up to `opensmi-client @ 9026539` (opensmi-core 0.7.1, opensmi-server 0.3.1) and, as 2.0.0,
fully replace the old SkillSharp.Client. In addition (user request): automated tests against all
example servers of opensmi-server.

## Decisions

- Seams as before (map): public API against the real opensmi-server; pure functions as unit tests
  (`ObjectPath`/`PathSegments`, `SampleUpdates`). Worked with the skill `tdd` in vertical slices.
- Paths = `ObjectPath` as in core 0.7 (folders, unique segments, aliases, Resolve/Find). Oracle:
  `oracle/dump_paths.py` produces the object list of the Python client for each server; the C# tests compare
  `Find("/**")` against it (dummy, upstream test server `servers/paths_server.py`, check, dictionary_entries,
  quick_start, rsl).
- Example servers run unchanged through `servers/run_example.py` (it only sets the port).
- Values/states as `Sample<T>`; component state `MachineryItemState?`; DTOs are dropped as upstream.
- New deviation: the skill/component state is read while browsing (otherwise it is still Halted/null after connect,
  which made the examples flaky). `ReadAllAsync` by segment.
- opensmi-server 0.3 releases a user's lock up to 1 s after *any* session of that user is closed
  → tests wait after each test (`ServerFixture.SettleAsync`).
- `Requirements`: server 0.3 attaches them to the skill object; Python reads them only under `SkillExecution` and
  finds nothing. User decision: fill them. The skill reads both places, checks do not inherit them (DEVIATIONS).
- Package: PackageId `SkillSharp.Client` 2.0.0 (user decision), namespace/assembly `OpenSmi.Client`.
  `legacy/` deleted, tag `skillsharp-legacy-final` set on `e57b3c0`.
- CI: actions on Node 24 (issue #10), package as an artifact. The docs rework from PR #2 (`opensmi-port`,
  commits cf50687, b62ed14) was taken over; its code part had already been solved differently here.
