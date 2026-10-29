# 05 Skelett und Server-Fixture

Type: task
Status: resolved
Blocked by: 02

## Question

Neue Solution `OpenSmi.Client.slnx` mit `src/OpenSmi.Client` und `tests/OpenSmi.Client.Tests`,
`Directory.Build.props` (Nullable, TreatWarningsAsErrors) und `.gitignore`. Den Altcode nach
`legacy/` verschieben. Die xUnit-Fixture startet `python main.py` aus
`opensmi-server/examples/dummy` (Pfad per `OPENSMI_SERVER_DIR`), wartet auf Port 4843 und beendet
den Prozess am Ende.

Ponytail: kein separates IntegrationTests-Projekt, Unit- und Integrationstests liegen in einem
Testprojekt.

## Akzeptanz

- `dotnet build` ohne Warnungen
- ein Test verbindet sich mit dem gestarteten Server

## Answer

Solution `OpenSmi.Client.slnx`, `Directory.Build.props` (net10.0, Nullable, TreatWarningsAsErrors), `.gitignore`. Der Altcode liegt jetzt in `legacy/` (kein `git mv`, das Repo ist kaputt, siehe 04). Die Fixture `tests/OpenSmi.Client.Tests/ServerFixture.cs` startet den Dummy-Server nur, wenn Port 4843 frei ist. Test-Pakete sind auf den lokalen NuGet-Cache gepinnt (Test.Sdk 17.14.1, xunit 2.9.3, runner 3.1.4), weil kein Feed erreichbar war.
