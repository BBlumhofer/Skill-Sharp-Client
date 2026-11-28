# 27 Best practice for notifications in .NET libraries

Type: research
Status: resolved
Blocked by: none

## Question

For ticket 25: how do established .NET libraries with comparable scenarios
(long-lived connection, server pushes value changes, state machines, several subscribers)
report events to the user? And what do Microsoft's Framework Design Guidelines recommend?

The comparison covers .NET events, `IObservable<T>`/System.Reactive and
`IAsyncEnumerable<T>`/channels: threading, ordering, exception behavior, async handlers,
unsubscribing, backpressure, dependencies, testability and ergonomics for users (UI, worker service,
scripts).

At minimum, these are examined: OPC Foundation UA-.NETStandard (MonitoredItem.Notification,
Session.KeepAlive), a commercial OPC UA client with a "modern" API (e.g. Workstation.UaClient /
Traeger, Opc.UaFx), MQTTnet (v4/v5, `ApplicationMessageReceivedAsync`), Microsoft.Azure.Devices.Client,
SignalR client (`On<T>`/streaming), gRPC streaming, NATS.Client.Core (`SubscribeAsync` as
`IAsyncEnumerable`), StackExchange.Redis Pub/Sub, RabbitMQ.Client 7 (`AsyncEventHandler`), Orleans
Streams, Akka.NET. For each: which pattern, why (release notes or design docs), and whether it
was switched (e.g. MQTTnet from handlers to async events, RabbitMQ to `AsyncEventingBasicConsumer`).

## Answer

Comparable network clients have moved from sync handlers to Task-returning events (MQTTnet 4, RabbitMQ 7, Azure IoT v2). SignalR, NATS and Azure.Core do the same. There the handlers run sequentially from a single-reader queue, and exceptions are isolated per handler.
For waiting and consuming they also offer `IAsyncEnumerable` over bounded channels (NATS, gRPC, SignalR streaming, Redis, OPC Foundation 2.0 `IStreamingSubscription` with `TakeUntilAsync` for "state-machine waits"). Only Workstation.UaClient has Rx as its main API.
The FDG (2008) recommend `EventHandler<T>` with a void return. Newer Microsoft clients (SignalR, Azure.Core) depart from that, explicitly to avoid `async void`.
Recommendation (departing from ticket 25): a hybrid of `event AsyncEventHandler<XxxEventArgs>` (sequential, errors logged per handler), `Watch…Async` as `IAsyncEnumerable` (current value first, DropOldest buffer) and the existing `WaitFor…Async`. No Rx.
What changes: `AsyncSignal` is dropped, the handlers run sequentially instead of via WhenAll, the signatures become `(sender, EventArgs)`. Handlers that wait for future notifications of the same client block the dispatch loop (already true today) and will have to solve that through Watch/WaitFor.
Details: [../research/27-benachrichtigung-best-practice.md](../research/27-benachrichtigung-best-practice.md)
