# 25 Benachrichtigungsmodell: AsyncSignal, .NET-Events, Rx oder IAsyncEnumerable

Type: grilling
Status: resolved
Blocked by: 27

## Question

Wie meldet der Client Zustandsänderungen (Status, Skill-Zustand, Variablenwert, Lock, Notifications)?
Es besteht keine Bindung an `AsyncSignal<T>`. Bewertung und Empfehlung: siehe Kommentar.

## Comments

**Agent, 2026-10-29 – Bewertung**

| Option | Pro | Contra |
|---|---|---|
| `AsyncSignal<T>` (heute) | nah an Python, async Handler | Eigenbau; await im seriellen Dispatch lässt einen langsamen Handler alle Notifications aufhalten; fremd für .NET-Nutzer |
| `event EventHandler<T>` | idiomatisch, null Abhängigkeiten, mit Rx (`FromEventPattern`) und jedem UI-Framework nutzbar, `sender` ist schon da | Handler synchron (async-Arbeit: selbst starten) |
| `IObservable<T>` + System.Reactive | mächtig zum Kombinieren | neue Abhängigkeit, Lernkurve, für Statuswechsel überdimensioniert |
| `IAsyncEnumerable<T>` je Abo | Pull, Backpressure, Abbruch per Token | jeder Nutzer braucht eine Schleife, Mehrfach-Abos brauchen eigene Puffer; passt schlecht zu „mehrere Handler“ |

Empfehlung: **.NET-Events** (`event EventHandler<SkillState>? StateChanged` usw.), ausgelöst aus dem
seriellen Dispatch-Loop (nie auf dem SDK-Thread), Ausnahmen pro Handler geloggt und isoliert.
`AsyncSignal` entfällt. Intern nutzt `AutoLock` die Events, Waits nutzen weiter TaskCompletionSource.

## Answer

Mit dem Nutzer entschieden (2026-10-29), gestützt auf die Recherche
[27 Best Practice Benachrichtigungen](27-benachrichtigung-best-practice.md):
1. `AsyncSignal` entfällt. Neu: `event AsyncEventHandler<XxxEventArgs>` (Handler geben `Task` zurück),
   `WatchXxxAsync(ct)` als `IAsyncEnumerable`, die `WaitFor…Async`-Methoden bleiben. Kein Rx.
2. Handler laufen **nacheinander** aus der seriellen Dispatch-Schleife; ein Fehler wird pro Handler geloggt.
3. Watch-Streams liefern zuerst den aktuellen Wert, jeder Aufrufer hat einen kleinen Puffer, bei Überlauf fliegt das Älteste raus.
4. Unit-Enum in PascalCase (`Centimetre`), der Python-Name bleibt in `EUInformation.Description`.
Umsetzung: [29 Benachrichtigungsmodell umsetzen](29-benachrichtigungsmodell-umsetzen.md).
