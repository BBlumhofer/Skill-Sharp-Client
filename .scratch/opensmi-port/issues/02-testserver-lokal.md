# 02 Test server can be started locally

Type: task
Status: resolved
Blocked by: none

## Question

Does `opensmi-server` run locally so that integration tests and the Python oracle can run against it?

## Answer

Yes. `opensmi-server` (547ce5b) is installed as editable in Python 3.12.
`cd ..\opensmi-server\examples\dummy; python main.py` serves `opc.tcp://localhost:4843/server` with
`DummyMachine` (7 skills, `DummyComponent` → `NestedDummyComponent`, `DummyMethod`). The oracle runs
with `PYTHONPATH` pointing at the reference checkout e8740c2. The pip-installed `OpenSMI-Client 1.0.0rc2`
is incompatible.
