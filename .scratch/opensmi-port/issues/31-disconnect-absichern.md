# 31 Make DisconnectAsync safe against concurrent calls

Type: task
Status: resolved
Blocked by: 30

## Question

`DisconnectAsync` is not protected against concurrent calls, for example when a dead server without
AutoReconnect starts the teardown and the user calls `DisconnectAsync` at the same time (see 28).
Two calls should trigger exactly one teardown; both return only once `Disconnected` is reached.
The item retry (SPEC §7.1) deliberately stays open until a real server shows the problem.

## Acceptance

- two concurrent `DisconnectAsync` calls → exactly one sequence `Disconnecting` → `Disconnected`, no exception

## Answer

`DisconnectAsync` now shares one teardown task: the first call starts `Disconnecting` → teardown →
`Disconnected`, all further calls (including the keep-alive teardown without AutoReconnect and `DisposeAsync`) wait
for the same task. If a `StatusChanged` handler for `Disconnecting` calls `DisconnectAsync` itself, the
call returns immediately instead of waiting for the teardown that is waiting for it (no new deadlock). After
the teardown, the same instance can connect again with `ConnectAsync`, as before. Test:
`RemoteServerTests.Concurrent_disconnects_tear_down_once_and_the_instance_reconnects`.
