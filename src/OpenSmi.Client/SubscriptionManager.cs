using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;

namespace OpenSmi.Client;

/// <summary>
/// Port of <c>core/subscription_manager.py</c>: one shared subscription per server, one monitored item per
/// NodeId carrying any number of handlers. A heartbeat rebuilds the subscription with all items when no
/// notification arrived for <c>HeartbeatStaleAfter</c> (the CurrentTime item ticks constantly). Notifications
/// are dispatched serially on one channel, so the order per node is kept and handlers never run on the SDK
/// publish thread.
/// </summary>
// shortcut: no unsubscribe yet; the Python client never unsubscribes.
internal sealed class SubscriptionManager : IAsyncDisposable
{
    private Subscription _subscription;
    private readonly RemoteServer _server;
    private readonly TimeSpan _staleAfter;
    private readonly ILogger _logger;
    private readonly Dictionary<NodeId, List<Func<DataValue, Task>>> _dataHandlers = [];
    private readonly Dictionary<NodeId, List<Func<EventFieldList, Task>>> _eventHandlers = [];
    private readonly Channel<Func<Task>> _queue = Channel.CreateUnbounded<Func<Task>>(new() { SingleReader = true });
    private readonly Task _pump;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _heartbeat;
    private long _lastNotification = Environment.TickCount64;

    // Select clauses of every event item; NotificationEvent parsing relies on this order.
    internal static readonly string[] EventFields = ["Message", "Severity", "SourceNode", "ErrorCode"];

    private SubscriptionManager(Subscription subscription, RemoteServer server, TimeSpan staleAfter)
    {
        _subscription = subscription;
        _server = server;
        _staleAfter = staleAfter;
        _logger = server.Logger;
        subscription.Handle = this; // survives the SDK's subscription cloning on session recreate
        _pump = Task.Run(PumpAsync);
        _heartbeat = Task.Run(HeartbeatAsync);
    }

    public static async Task<SubscriptionManager> CreateAsync(RemoteServer server, TimeSpan publishingInterval,
        TimeSpan heartbeatStaleAfter, CancellationToken ct)
    {
        var subscription = await NewSubscriptionAsync(server.RequireSession(), (int)publishingInterval.TotalMilliseconds,
            server.Logger, ct).ConfigureAwait(false);
        return new SubscriptionManager(subscription, server, heartbeatStaleAfter);
    }

    private static async Task<Subscription> NewSubscriptionAsync(ISession session, int publishingInterval, ILogger logger, CancellationToken ct)
    {
        var subscription = new Subscription(session.DefaultSubscription)
        {
            PublishingInterval = publishingInterval,
            PublishingEnabled = true,
            KeepAliveCount = 10,
            LifetimeCount = 100,
        };
        session.AddSubscription(subscription);
        await subscription.CreateAsync(ct).ConfigureAwait(false);
        if (subscription.CurrentPublishingInterval != subscription.PublishingInterval)
            logger.LogWarning("Server revised publishing interval to {Interval} ms", subscription.CurrentPublishingInterval);
        return subscription;
    }

    /// <summary>After a session recreate the SDK holds a clone of our subscription; handlers are cloned with it.</summary>
    public void Rebind(ISession session) =>
        _subscription = session.Subscriptions.Single(s => ReferenceEquals(s.Handle, this));

    public uint MonitoredItemCount => _subscription.MonitoredItemCount;

    /// <summary>The server sends the current value right after an item is created, so handlers see an initial value.</summary>
    public async Task SubscribeDataChangeAsync(IEnumerable<(NodeId Node, Func<DataValue, Task> Handler)> items, CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            lock (_dataHandlers)
            {
                foreach (var (node, handler) in items)
                {
                    if (_dataHandlers.TryGetValue(node, out var handlers))
                    {
                        handlers.Add(handler);
                        continue;
                    }
                    _dataHandlers[node] = [handler];
                    _subscription.AddItem(DataItem(node));
                }
            }
            await ApplyAsync(ct).ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }

    public async Task SubscribeEventAsync(NodeId sourceNode, Func<EventFieldList, Task> handler, CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            lock (_eventHandlers)
            {
                if (_eventHandlers.TryGetValue(sourceNode, out var handlers))
                {
                    handlers.Add(handler);
                    return;
                }
                _eventHandlers[sourceNode] = [handler];
                _subscription.AddItem(EventItem(sourceNode));
            }
            await ApplyAsync(ct).ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }

    private MonitoredItem DataItem(NodeId node)
    {
        var item = new MonitoredItem(_subscription.DefaultItem)
        {
            StartNodeId = node,
            AttributeId = Opc.Ua.Attributes.Value,
            SamplingInterval = _subscription.PublishingInterval,
            QueueSize = 10,
        };
        item.Notification += (_, e) =>
        {
            if (e.NotificationValue is MonitoredItemNotification n) Enqueue(_dataHandlers, node, n.Value);
        };
        return item;
    }

    private MonitoredItem EventItem(NodeId sourceNode)
    {
        var filter = new EventFilter();
        foreach (var field in EventFields) filter.AddSelectClause(ObjectTypeIds.BaseEventType, field);
        var item = new MonitoredItem(_subscription.DefaultItem)
        {
            StartNodeId = sourceNode,
            AttributeId = Opc.Ua.Attributes.EventNotifier,
            NodeClass = NodeClass.Object,
            Filter = filter,
            QueueSize = 100,
        };
        item.Notification += (_, e) =>
        {
            // Route by SourceNode like Python: some servers emit everything from the Server object.
            if (e.NotificationValue is EventFieldList f && f.EventFields.Count == EventFields.Length
                && f.EventFields[2].Value is NodeId source)
                Enqueue(_eventHandlers, source, f);
        };
        return item;
    }

    private async Task ApplyAsync(CancellationToken ct)
    {
        await _subscription.ApplyChangesAsync(ct).ConfigureAwait(false);
        foreach (var item in _subscription.MonitoredItems.Where(i => ServiceResult.IsBad(i.Status.Error)))
            _logger.LogWarning("Monitored item {Node} failed: {Error}", item.StartNodeId, item.Status.Error);
    }

    private async Task HeartbeatAsync()
    {
        try
        {
            while (true)
            {
                await Task.Delay(_staleAfter / 4, _stop.Token).ConfigureAwait(false);
                // While (re)connecting the SDK owns the subscription; start counting once connected.
                if (_server.Status != RemoteServerStatus.Connected) Touch();
                if (Environment.TickCount64 - Interlocked.Read(ref _lastNotification) <= _staleAfter.TotalMilliseconds) continue;
                _logger.LogWarning("Heartbeat stale, rebuilding subscription with {Count} items", MonitoredItemCount);
                try { await RebuildAsync(_stop.Token).ConfigureAwait(false); }
                catch (Exception e) when (e is not OperationCanceledException) { _logger.LogWarning(e, "Could not rebuild subscription"); }
            }
        }
        catch (OperationCanceledException) { }
    }

    /// <summary>Deletes the subscription and creates a new one with every data and event item.</summary>
    private async Task RebuildAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var session = _server.RequireSession();
            var old = _subscription;
            await old.DeleteAsync(true, ct).ConfigureAwait(false); // silent: the server may have dropped it already
            await session.RemoveSubscriptionAsync(old, ct).ConfigureAwait(false);
            var subscription = await NewSubscriptionAsync(session, old.PublishingInterval, _logger, ct).ConfigureAwait(false);
            subscription.Handle = this;
            _subscription = subscription;
            lock (_dataHandlers)
                foreach (var node in _dataHandlers.Keys) subscription.AddItem(DataItem(node));
            lock (_eventHandlers)
                foreach (var node in _eventHandlers.Keys) subscription.AddItem(EventItem(node));
            await ApplyAsync(ct).ConfigureAwait(false);
            Touch();
            _logger.LogInformation("Rebuilt subscription");
        }
        finally { _gate.Release(); }
    }

    /// <summary>Runs <paramref name="work"/> on the dispatch loop, after the notifications queued so far.</summary>
    public void Post(Func<Task> work) => _queue.Writer.TryWrite(work);

    private void Touch() => Interlocked.Exchange(ref _lastNotification, Environment.TickCount64);

    private void Enqueue<T>(Dictionary<NodeId, List<Func<T, Task>>> registry, NodeId node, T value)
    {
        Touch();
        Func<T, Task>[] handlers;
        lock (registry)
        {
            if (!registry.TryGetValue(node, out var list)) return;
            handlers = [.. list];
        }
        foreach (var h in handlers) _queue.Writer.TryWrite(() => h(value));
    }

    private async Task PumpAsync()
    {
        await foreach (var work in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try { await work().ConfigureAwait(false); }
            catch (Exception e) { _logger.LogError(e, "Subscription handler failed"); }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        await _heartbeat.ConfigureAwait(false);
        _queue.Writer.TryComplete();
        try { await _subscription.DeleteAsync(true, CancellationToken.None).ConfigureAwait(false); }
        catch (ServiceResultException e) { _logger.LogWarning(e, "Deleting subscription failed"); }
        await _pump.ConfigureAwait(false);
    }
}
