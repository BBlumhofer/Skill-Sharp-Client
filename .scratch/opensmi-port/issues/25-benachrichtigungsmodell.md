# 25 Benachrichtigungsmodell: AsyncSignal, .NET-Events, Rx oder IAsyncEnumerable

Type: grilling
Status: open (HITL)
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
