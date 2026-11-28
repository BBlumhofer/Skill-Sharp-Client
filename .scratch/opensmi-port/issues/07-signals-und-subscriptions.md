# 07 AsyncSignal and SubscriptionManager

Type: task
Status: resolved
Blocked by: 06

## Question

`AsyncSignal<T>`: handlers run concurrently; exceptions are logged and not propagated.
Internal `SubscriptionManager`: one subscription per server, dedup per NodeId, several handlers per
item, chunking by `MaxMonitoredItemsPerCall`, processing of the delivered values (no re-read),
event items with routing by `SourceNode`. `RemoteServer.TimeUpdated` via CurrentTime. Oracle:
`core/subscription_manager.py`.

## Acceptance

- Unit: an exception in one handler does not reach the sender, the other handlers run.
- Integration: `TimeUpdated` fires within 3 s.

## Answer

`AsyncSignal.cs` and `SubscriptionManager.cs`: one subscription, dedup per NodeId, serial dispatch through a channel, events are routed by SourceNode. Deviation: the SDK does the chunking in `ApplyChangesAsync`. Unsubscribing (`IAsyncDisposable`) is not built; the Python client never unsubscribes either. Heartbeat rebuild → 18.
