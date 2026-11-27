# 34 Version 2.0.0: Upstream 9026539, opensmi-server 0.3.1, SkillSharp ersetzt

Type: task
Status: done (2026-11-27)

## Question

Den Client auf `opensmi-client @ 9026539` (opensmi-core 0.7.1, opensmi-server 0.3.1) nachziehen und als 2.0.0
den alten SkillSharp.Client vollständig ersetzen. Zusätzlich (Nutzerwunsch): automatisierte Tests gegen alle
Beispielserver von opensmi-server.

## Decisions

- Seams wie bisher (Map): öffentliche API gegen echten opensmi-server; reine Funktionen als Unit-Tests
  (`ObjectPath`/`PathSegments`, `SampleUpdates`). Gearbeitet mit dem Skill `tdd` in vertikalen Slices.
- Pfade = `ObjectPath` wie core 0.7 (Ordner, eindeutige Segmente, Aliase, Resolve/Find). Orakel:
  `oracle/dump_paths.py` erzeugt für jeden Server die Objektliste des Python-Clients; die C#-Tests vergleichen
  `Find("/**")` damit (Dummy, Upstream-Testserver `servers/paths_server.py`, check, dictionary_entries,
  quick_start, rsl).
- Beispielserver laufen unverändert über `servers/run_example.py` (setzt nur den Port).
- Werte/Zustände als `Sample<T>`; Komponentenzustand `MachineryItemState?`; DTOs entfallen wie upstream.
- Abweichung neu: Skill-/Komponentenzustand wird beim Browsen gelesen (sonst nach Connect noch Halted/null,
  was Beispiele flaky machte). `ReadAllAsync` nach Segment.
- opensmi-server 0.3 gibt den Lock eines Users bis zu 1 s nach dem Schließen *irgendeiner* Session dieses Users
  frei → Tests warten nach jedem Test (`ServerFixture.SettleAsync`).
- `Requirements`: Server 0.3 hängt sie an das Skill-Objekt, Python liest sie nur unter `SkillExecution` und
  findet nichts. Nutzerentscheidung: füllen. Der Skill liest beide Stellen, Checks erben sie nicht (DEVIATIONS).
- Paket: PackageId `SkillSharp.Client` 2.0.0 (Nutzerentscheidung), Namespace/Assembly `OpenSmi.Client`.
  `legacy/` gelöscht, Tag `skillsharp-legacy-final` auf `e57b3c0` gesetzt.
- CI: Actions auf Node 24 (Issue #10), Paket als Artefakt. Doku-Überarbeitung aus PR #2 (`opensmi-port`,
  Commits cf50687, b62ed14) übernommen; dessen Code-Teil war hier schon anders gelöst.
