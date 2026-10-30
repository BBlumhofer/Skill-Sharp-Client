# 03 Offene Entscheidungen aus SPEC §11

Type: grilling
Status: resolved
Blocked by: –

## Question

1. Paket/Repo: `OpenSmi.Client` in diesem Repo oder neues Repo `opensmi-client-dotnet`?
2. Nur `net10.0` oder zusätzlich `net8.0`?
3. Wird das SmartFactory-Paket (P6) gebraucht?
4. `AsyncSignal<T>` oder `IObservable`/`IAsyncEnumerable`?
5. `Unit`-Enum generieren oder weglassen?

## Arbeitsannahme bis zur Klärung

Bis zur Klärung gelten die Vorschläge der SPEC, die sich alle leicht ändern lassen:
`OpenSmi.Client` in diesem Repo, nur `net10.0`, kein SF-Paket, `AsyncSignal<T>`, kein
`Unit`-Enum.

## Answer

Entschieden mit dem Nutzer (2026-10-29):
1. Bleibt in diesem Repo; Umbenennung später.
2. **Zusätzlich `net8.0`** → [24 Multi-Target net8.0/net10.0](24-multi-target-net8.md).
3. SmartFactory-Paket kommt **danach** als eigenes Vorhaben; diese Map liefert nur den Client.
4. Nicht festgeschrieben: „sauber neu beurteilen, keine Bindung an Bisheriges“ → [25 Benachrichtigungsmodell](25-benachrichtigungsmodell.md).
5. **`Unit`-Enum wird generiert** → [26 Unit-Enum generieren](26-unit-enum.md).
