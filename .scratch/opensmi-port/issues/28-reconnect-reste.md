# 28 Reconnect leftovers: watchdog option, AutoReconnect=false, item retry

Type: task
Status: resolved
Blocked by: 23, 29

## Question

Open points from [23 Reconnect](23-reconnect.md):
1. The check interval of 2 s is hard-coded; it should be configurable through `WatchdogInterval` (SPEC §4.1, default 1 s).
2. With `AutoReconnect = false` a dead server is not detected and the status stays `Connected`;
   `Disconnected` and `StatusChanged` are expected.
3. Failed individual monitored items are only logged and not retried with backoff (SPEC §7.1).
4. A short network drop with a reactivate of the same session is untested (only a server restart).

## Answer

1 Watchdog: `RemoteServerOptions.WatchdogInterval` (default 1 s) sets `Session.KeepAliveInterval`, also with
`AutoReconnect = false`. Test: `RemoteServerTests.Watchdog_interval_sets_the_keep_alive_period_default_one_second`.

2 AutoReconnect = false: the KeepAlive handler is now always attached to the session. Without a reconnect handler, a
bad KeepAlive in status `Connected` calls `DisconnectAsync` (via `Task.Run`, not on the SDK thread). This produces
`Disconnecting` → `Disconnected` along with `StatusChanged`, and the subscription and session are torn down as in a normal
disconnect. Test: `ReconnectTests.Without_auto_reconnect_a_dead_server_ends_disconnected` (port 4922).

3 Item retry: skipped. I found no honest way to provoke it. The dummy server creates no
nodes at runtime, users may not call AddNodes (only `UserRole.Admin`), and the only realistic scenario (a restart where the
client connects before the address space is ready) cannot be hit deterministically. A test with a
non-existent NodeId would only show that a retry is attempted, not that it fixes anything. Failed
items are still only logged; the heartbeat rebuild includes them when stale.

4 Reactivate: new test helper `TcpProxy` (tests) between the client and `TestServer`; `Cut()` closes all
connections and accepts no new ones, `Restore()` opens again. Test
`ReconnectTests.Short_network_drop_reactivates_the_same_session` (server 4923, proxy 4924): `Reconnecting` →
`Connected`, the same `Session` instance, the same machine, `ValueChanged` fires again. Production code needed
no change for this; the path in `OnReconnected` was already correct.
