# 26 Generate unit enum

Type: task
Status: resolved
Blocked by: none

## Question

Decision 03: the roughly 2100 UN/CEFACT Rec 20 units from `opensmi-core/unit_all.py` are generated as a
C# enum, not written by hand. This includes a generator script under `tools/`, the
generated file `src/OpenSmi.Client/Unit.g.cs` and the helpers that Python offers (`symbol`,
`from_ua_unit_id`, `from_symbol`, `ua_unit_id`, `ua_eu_information`, see `core/unit_base.py`),
as well as `RemoteVariable.Unit` → `Unit?` as an additional mapping. `EUInformation` is kept.

## Acceptance

- Unit: `Unit.FromUaUnitId(<id of "cm">)` → symbol `cm`; round trip via `ToEuInformation`
- Integration: `DummyIntParameter` yields the unit "centimetre"

## Answer

API: `enum Unit` (names as in Python, e.g. `CENTIMETRE`; value = UA unit id, so `ua_unit_id` = `(int)unit`) plus
the static class `Units`: `FromUaUnitId`, `FromSymbol` (unknown/ambiguous → `ArgumentException`, like Python's
`ValueError`), extensions `unit.Symbol()`, `unit.ToEuInformation()` (NamespaceUri cefact, DisplayName = symbol,
Description = name) and `euInformation.ToUnit()`. `RemoteVariable.Unit` stays `EUInformation?`; the mapping is
`variable.Unit?.ToUnit()`. No second property in `RemoteVariable`, because there `Unit` would hide the type `Unit`.
A `Unit.FromUaUnitId` is not possible on an enum, hence `Units.FromUaUnitId`. Python aliases (2 of them) are
not generated, so that every enum value is unique.

Files: `tools/generate_units.py` (imports the installed opensmi-core, writes the version into the header),
`src/OpenSmi.Client/Unit.g.cs` (2133 units), `src/OpenSmi.Client/Units.cs`.

Tests: `tests/OpenSmi.Client.Tests/UnitTests.cs`: cm ↔ 4410708, round trip via `ToEuInformation`, unknown id,
`FromSymbol` unique/unknown/ambiguous ("cal"), integration `DummyIntParameter` → `Unit.CENTIMETRE`. 5/5 green.
