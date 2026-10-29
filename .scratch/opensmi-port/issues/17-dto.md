# 17 DTOs

Type: task
Status: resolved
Blocked by: 11, 12, 13, 15

## Question

`ToDto()` als `record`s, serialisierbar mit `System.Text.Json`. Die Struktur folgt `client/dto.py`
plus den Abweichungen aus SPEC §10 (`LockPriority`, `Lock = null`, Feasibility/Precondition im DTO).

## Akzeptanz (Integration)

- Pfade und NodeIds aller Variablen im DTO von `DummyMachine` stimmen mit der Orakel-Ausgabe überein

## Answer

`Dto.cs` (Records, `ToDto()` als Extensions). Test: Alle 113 Pfad/NodeId-Paare des Python-Orakels (`oracle/dummy-machine-paths.txt`, erzeugt mit `oracle/dump_paths.py`) kommen auch im C#-DTO vor.
