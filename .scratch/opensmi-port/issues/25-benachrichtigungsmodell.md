# 25 Notification model: AsyncSignal, .NET events, Rx or IAsyncEnumerable

Type: grilling
Status: resolved
Blocked by: 27

## Question

How does the client report state changes (status, skill state, variable value, lock, notifications)?
There is no commitment to `AsyncSignal<T>`. Assessment and recommendation: see the comment.

## Comments

Agent, 2026-10-29: assessment

| Option | Pro | Con |
|---|---|---|
| `AsyncSignal<T>` (today) | close to Python, async handlers | home-grown; awaiting in the serial dispatch lets one slow handler hold up all notifications; unfamiliar to .NET users |
| `event EventHandler<T>` | idiomatic, zero dependencies, usable with Rx (`FromEventPattern`) and any UI framework, `sender` is already there | handlers are synchronous (async work: start it yourself) |
| `IObservable<T>` + System.Reactive | powerful for composition | new dependency, learning curve, oversized for status changes |
| `IAsyncEnumerable<T>` per subscription | pull, backpressure, cancellation by token | every user needs a loop, multiple subscriptions need their own buffers; fits "several handlers" poorly |

Recommendation: .NET events (`event EventHandler<SkillState>? StateChanged` etc.), raised from the
serial dispatch loop (never on the SDK thread), exceptions logged and isolated per handler.
`AsyncSignal` is dropped. Internally `AutoLock` uses the events; waits keep using TaskCompletionSource.

## Answer

Decided with the user (2026-10-29), based on the research
[27 Best practice for notifications](27-benachrichtigung-best-practice.md):
1. `AsyncSignal` is dropped. New: `event AsyncEventHandler<XxxEventArgs>` (handlers return `Task`),
   `WatchXxxAsync(ct)` as `IAsyncEnumerable`, the `WaitFor…Async` methods stay. No Rx.
2. Handlers run one after the other from the serial dispatch loop; an error is logged per handler.
3. Watch streams deliver the current value first, each caller has a small buffer, on overflow the oldest entry is dropped.
4. Unit enum in PascalCase (`Centimetre`), the Python name stays in `EUInformation.Description`.
Implementation: [29 Implement the notification model](29-benachrichtigungsmodell-umsetzen.md).
