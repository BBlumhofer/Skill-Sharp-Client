# 02 Testserver lokal startbar

Type: task
Status: resolved
Blocked by: –

## Question

Läuft `opensmi-server` lokal so, dass Integrationstests und das Python-Orakel dagegen fahren können?

## Answer

Ja. `opensmi-server` (547ce5b) ist editable in Python 3.12 installiert.
`cd ..\opensmi-server\examples\dummy; python main.py` liefert `opc.tcp://localhost:4843/server` mit
`DummyMachine` (7 Skills, `DummyComponent` → `NestedDummyComponent`, `DummyMethod`). Das Orakel läuft
mit `PYTHONPATH` auf den Referenz-Checkout e8740c2. Das pip-installierte `OpenSMI-Client 1.0.0rc2`
ist inkompatibel.
