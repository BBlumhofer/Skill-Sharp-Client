# 26 Unit-Enum generieren

Type: task
Status: resolved
Blocked by: –

## Question

Entscheidung 03: Die ca. 2100 UN/CEFACT-Rec-20-Einheiten aus `opensmi-core/unit_all.py` werden als
C#-Enum generiert, nicht von Hand geschrieben. Dazu kommen ein Generator-Skript unter `tools/`, die
generierte Datei `src/OpenSmi.Client/Unit.g.cs` und die Helfer, die Python bietet (`symbol`,
`from_ua_unit_id`, `from_symbol`, `ua_unit_id`, `ua_eu_information`, siehe `core/unit_base.py`)
sowie `RemoteVariable.Unit` → `Unit?` als zusätzliche Abbildung. `EUInformation` bleibt erhalten.

## Akzeptanz

- Unit: `Unit.FromUaUnitId(<id von „cm“>)` → Symbol `cm`; Round-Trip über `ToEuInformation`
- Integration: `DummyIntParameter` ergibt die Unit „Zentimeter“

## Answer

API: `enum Unit` (Namen wie in Python, z. B. `CENTIMETRE`; Wert = UA-Unit-Id, also `ua_unit_id` = `(int)unit`) plus
statische Klasse `Units`: `FromUaUnitId`, `FromSymbol` (unbekannt/mehrdeutig → `ArgumentException`, wie Pythons
`ValueError`), Extensions `unit.Symbol()`, `unit.ToEuInformation()` (NamespaceUri cefact, DisplayName = Symbol,
Description = Name) und `euInformation.ToUnit()`. `RemoteVariable.Unit` bleibt `EUInformation?`; die Abbildung ist
`variable.Unit?.ToUnit()`. Kein zweites Property in `RemoteVariable`, weil dort `Unit` den Typ `Unit` verdecken würde.
Ein `Unit.FromUaUnitId` ist bei einem Enum nicht möglich, daher `Units.FromUaUnitId`. Python-Aliase (2 Stück) werden
nicht generiert, damit jeder Enum-Wert eindeutig ist.

Dateien: `tools/generate_units.py` (importiert das installierte opensmi-core, schreibt die Version in den Header),
`src/OpenSmi.Client/Unit.g.cs` (2133 Einheiten), `src/OpenSmi.Client/Units.cs`.

Tests: `tests/OpenSmi.Client.Tests/UnitTests.cs`: cm ↔ 4410708, Round-Trip über `ToEuInformation`, unbekannte Id,
`FromSymbol` eindeutig/unbekannt/mehrdeutig („cal“), Integration `DummyIntParameter` → `Unit.CENTIMETRE`. 5/5 grün.
