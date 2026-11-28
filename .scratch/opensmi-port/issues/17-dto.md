# 17 DTOs

Type: task
Status: resolved
Blocked by: 11, 12, 13, 15

## Question

`ToDto()` as `record`s, serializable with `System.Text.Json`. The structure follows `client/dto.py`
plus the deviations from SPEC §10 (`LockPriority`, `Lock = null`, feasibility/precondition in the DTO).

## Acceptance (integration)

- paths and NodeIds of all variables in the DTO of `DummyMachine` match the oracle output

## Answer

`Dto.cs` (records, `ToDto()` as extensions). Test: all 113 path/NodeId pairs of the Python oracle (`oracle/dummy-machine-paths.txt`, generated with `oracle/dump_paths.py`) also appear in the C# DTO.
