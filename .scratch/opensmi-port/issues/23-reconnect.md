# 23 Reconnect with preserved object identities and heartbeat rebuild

Type: task
Status: resolved
Blocked by: 21, 22

## Question

SPEC §4.1 and §7.1: there is only one reconnect mechanism, through the `SessionReconnectHandler`.
The status changes `Connected → Reconnecting → Connected`. After the reconnect, `Machines` and
all objects are the same instances (no new browse), and notifications keep arriving. The
heartbeat via `CurrentTime` rebuilds the subscription with all data and event items
when nothing arrived for longer than `HeartbeatStaleAfter` (default 10 s). `AutoReconnect` and
`ReconnectMaxDelay` are added as options. How the SDK behaves is described in ticket 22.

## Acceptance (integration, own `TestServer`)

- stop the server → status `Reconnecting`; restart the server → status `Connected` within 30 s
- afterwards `ReferenceEquals` holds for the machine before and after the reconnect, and `ValueChanged` of a
  Monitoring variable fires again
- `DisconnectAsync` during `Reconnecting` ends cleanly with `Disconnected`

## Answer

Design (as ticket 22 recommends):
- `RemoteServerOptions`: `AutoReconnect` (true), `ReconnectMaxDelay` (30 s), `HeartbeatStaleAfter` (10 s).
- `RemoteServer` owns a `SessionReconnectHandler(reconnectAbort: true, ReconnectMaxDelay)`.
  `KeepAliveInterval` = 2000 ms. On a bad KeepAlive of the current session: `BeginReconnect(…, 1000)`,
  `CancelKeepAlive`, status `Reconnecting`. In the callback, a new session replaces `Session`, the
  old one is only `Dispose()`d, and `SubscriptionManager.Rebind` finds the cloned subscription via `Handle`.
  Then `Connected`. Objects stay the same, no new browse.
- `DisconnectAsync`/`TearDown` disposes the handler first; a callback after the disconnect is
  ignored and any session created in it is released.
- `SubscriptionManager`: every notification (data and event) sets the heartbeat timestamp. A
  loop checks every `HeartbeatStaleAfter/4`; time only counts in status `Connected`. When stale,
  the subscription is deleted (silently) and removed from the session, then created again with all data and
  event items. If that fails, the next tick tries again. A
  `SemaphoreSlim` serializes subscribe and rebuild. I removed the `shortcut:` comment about the rebuild;
  the one about unsubscribe stays.

Tests (`ReconnectTests`, own `TestServer`s on 4894 to 4896): restart → `Reconnecting` →
`Connected`, the same machine, `ValueChanged` fires again. `DisconnectAsync` during `Reconnecting`
ends in `Disconnected` and stays there, even when the server comes back. Heartbeat: the subscription
is deleted on the server side through the raw session; afterwards a new subscription with the same
item count appears, and `ValueChanged` fires again.

Deviations/risks:
- Not set: `TransferSubscriptionsOnReconnect`/`DeleteSubscriptionsOnClose`. The restart does not need
  them because the SDK recreates the subscription by itself.
- With `AutoReconnect = false` the client does not react to a dead server; the status stays
  `Connected`. Python would switch to `Disconnected` here. This is not built.
- `KeepAliveInterval` is fixed at 2 s. `WatchdogInterval` from SPEC §4.1 is not wired up.
- Single-item retry with backoff (SPEC §7.1) is not built; failed items are only logged.
- The reactivate path (same session) is not tested, only the recreate path through a server restart.
