# 07 AsyncSignal und SubscriptionManager

Type: task
Status: resolved
Blocked by: 06

## Question

`AsyncSignal<T>`: Handler laufen nebenläufig; Ausnahmen werden geloggt und nicht propagiert.
Interner `SubscriptionManager`: eine Subscription pro Server, Dedup je NodeId, mehrere Handler je
Item, Chunking nach `MaxMonitoredItemsPerCall`, Verarbeitung der gelieferten Werte (kein Re-Read),
Event-Items mit Routing per `SourceNode`. `RemoteServer.TimeUpdated` über CurrentTime. Orakel:
`core/subscription_manager.py`.

## Akzeptanz

- Unit: Eine Ausnahme in einem Handler erreicht den Sender nicht, die übrigen Handler laufen.
- Integration: `TimeUpdated` feuert innerhalb von 3 s.

## Answer

`AsyncSignal.cs` und `SubscriptionManager.cs`: eine Subscription, Dedup je NodeId, serieller Dispatch über einen Channel, Events routen per SourceNode. Abweichung: Das Chunking übernimmt das SDK beim `ApplyChangesAsync`. Abbestellen (`IAsyncDisposable`) ist **nicht** gebaut, der Python-Client bestellt auch nie ab. Heartbeat-Rebuild → 18.
