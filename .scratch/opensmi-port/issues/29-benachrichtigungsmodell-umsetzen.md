# 29 Implement the notification model

Type: task
Status: resolved
Blocked by: 25

## Question

Implement the decision from [25](25-benachrichtigungsmodell.md):
- `public delegate Task AsyncEventHandler<in TEventArgs>(object sender, TEventArgs e);` and per signal
  an `event` with `XxxEventArgs` instead of `AsyncSignal<(...)>`: RemoteServer `StatusChanged`, `TimeUpdated`;
  BaseRemoteComponent `StatusChanged`, `NotificationReceived`; RemoteSkill `StateChanged`; RemoteVariable
  `ValueChanged`; RemoteLock `LockedChanged`, `LockingUserChanged`, `RenewDue`; RemoteUser `UserPresenceChanged`.
- Raise one after the other, exception logged per handler; never on the SDK thread.
- `WatchStateAsync` (skill), `WatchValueAsync` (variable), `WatchStatusAsync` (server) as
  `IAsyncEnumerable`: current value first, bounded buffer per caller, drop the oldest.
- Delete `AsyncSignal`; switch `AutoLock`, examples, tests and README to the new model.
- Docs note (XML docs on the delegate and README): in a handler, do not wait for future
  notifications of the same client (e.g. `StartAsync(StartWait.Completion)`); use Watch/WaitFor for that.
- Generate the unit enum in PascalCase (`tools/generate_units.py`), `Description` stays the Python name.
- `DEVIATIONS.md`: add the signals as a deviation from Python.

## Acceptance

- Unit: a handler that throws does not hold up the next handler; the order of the handlers is the registration order.
- Unit: `Watch…` delivers the current value first; a slow consumer loses old values but gets the newest one.
- Integration: `WatchStateAsync` sees `Running` and `Completed` of a DummySkill run.
- The full suite is green, 0 warnings.

## Answer

Implemented (2026-10-29):
- `AsyncSignal` is gone. New in `AsyncEvents.cs`: `AsyncEventHandler<TEventArgs>` (XML docs with a note on the
  re-entrancy trap), one `sealed record XxxEventArgs` per signal, `RaiseAsync` (handlers one after the other in
  registration order, errors logged per handler) and `Watchers<T>` (per caller `Channel.CreateBounded(8, DropOldest)`,
  current value first, unsubscribing when the enumeration ends or is cancelled).
- Events: server `StatusChanged`/`TimeUpdated`, component `StatusChanged`/`NotificationReceived`, skill `StateChanged`,
  variable `ValueChanged`, lock `LockedChanged`/`LockingUserChanged`/`RenewDue`, user `UserPresenceChanged`.
- Streams: `RemoteSkill.WatchStateAsync`, `RemoteVariable.WatchValueAsync` (`Variant`), `RemoteServer.WatchStatusAsync`.
- Status changes from SDK callbacks (KeepAlive, reconnect) set `Status` immediately, but the handlers run through
  `SubscriptionManager.Post` on the dispatch loop instead of on the SDK thread.
- `AutoLock`, examples, tests, README (section "Watching changes") and `DEVIATIONS.md` switched over.
- `Unit` regenerated in PascalCase from `tools/generate_units.py`; `EUInformation.Description` stays the Python name.
- Tests: `EventTests` (order/isolation, current value first, loss of old values, unsubscribing),
  `SkillTests.Watch_state_sees_running_and_completed`.
- Open: `SPEC.md` still describes `AsyncSignal` (historical document, not updated). Watch streams do not end
  on their own on disconnect, only through the token.
