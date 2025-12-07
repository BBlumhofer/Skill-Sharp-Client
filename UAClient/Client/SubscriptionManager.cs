using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;

namespace UAClient.Client
{
    public interface IDataChangeHandler
    {
        void DataChangeNotification(NodeId nodeId, DataValue value, MonitoredItemNotificationEventArgs args);
    }

    public class SubscriptionManager : IDisposable, IAsyncDisposable
    {
        // prevent multiple concurrent forced reconnects from racing
        private readonly System.Threading.SemaphoreSlim _reconnectLock = new(1, 1);
        private DateTime _lastReconnectAttempt = DateTime.MinValue;
        private readonly TimeSpan _reconnectCooldown = TimeSpan.FromSeconds(1);

        private readonly UaClient _client;
        private readonly List<Subscription> _subscriptions = new();
        private readonly ConcurrentDictionary<int, MonitoredItem> _monitored = new();
        // stores high-level IDataChangeHandler subscriptions: key = clientHandle
        private readonly ConcurrentDictionary<int, (IDataChangeHandler handler, NodeId nodeId)> _handlers = new();
        // stores raw action handlers added via AddMonitoredItemAsync so they can be recreated on rebind
        private readonly ConcurrentDictionary<int, Action<MonitoredItem, MonitoredItemNotificationEventArgs>> _rawHandlers = new();
        private Subscription? _defaultSubscription;

        public SubscriptionManager(UaClient client)
        {
            _client = client;
            // eagerly create default subscription on construction (async init pattern not ideal, but ensures sub exists)
            try
            {
                var session = _client.Session;
                if (session != null)
                {
                    _defaultSubscription = new Subscription() { PublishingInterval = 1000 };
                    session.AddSubscription(_defaultSubscription);
                    // Note: CreateAsync must be awaited, but constructor can't be async.
                    // We'll ensure it's created lazily on first use in a helper method.
                }
            }
            catch { }
        }

        public void Dispose()
        {
            DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var s in _subscriptions.ToArray())
            {
                try { await s.DeleteAsync(true); } catch { }
            }
            _subscriptions.Clear();
        }

        public async Task<Subscription> CreateSubscriptionAsync(double publishingInterval = 1000)
        {
            var session = _client.Session ?? throw new InvalidOperationException("No session");
            // ensure default subscription exists
            await EnsureDefaultSubscriptionAsync();
            var sub = new Subscription() { PublishingInterval = (int)publishingInterval };
            session.AddSubscription(sub);
            await sub.CreateAsync();
            _subscriptions.Add(sub);
            return sub;
        }

        private async Task<Subscription> EnsureDefaultSubscriptionAsync()
        {
            if (_defaultSubscription != null && _defaultSubscription.Created) return _defaultSubscription;
            var session = _client.Session ?? throw new InvalidOperationException("No session");
            if (_defaultSubscription == null)
            {
                _defaultSubscription = new Subscription() { PublishingInterval = 1000 };
                session.AddSubscription(_defaultSubscription);
            }
            if (!_defaultSubscription.Created)
            {
                await _defaultSubscription.CreateAsync();
                _subscriptions.Add(_defaultSubscription);
            }
            return _defaultSubscription;
        }

        public async Task<MonitoredItem> AddMonitoredItemAsync(NodeId nodeId, Action<MonitoredItem, MonitoredItemNotificationEventArgs> handler, Subscription? subscription = null, uint samplingInterval = 0)
        {
            var session = _client.Session ?? throw new InvalidOperationException("No session");
            var sub = subscription ?? await EnsureDefaultSubscriptionAsync();

            var mi = new MonitoredItem(sub.DefaultItem)
            {
                StartNodeId = nodeId,
                AttributeId = Attributes.Value,
                SamplingInterval = (int)samplingInterval,
                QueueSize = 10,
                DiscardOldest = true
            };

            mi.Notification += (m, e) => handler(m, e);
            sub.AddItem(mi);
            await ApplySubscriptionChangesWithRetryAsync(sub);

            _monitored[(int)mi.ClientHandle] = mi;
            _rawHandlers[(int)mi.ClientHandle] = handler;
            return mi;
        }

        public async Task<MonitoredItem> AddEventMonitoredItemAsync(NodeId nodeId, Action<MonitoredItem, MonitoredItemNotificationEventArgs> handler, Subscription? subscription = null)
        {
            var session = _client.Session ?? throw new InvalidOperationException("No session");
            var sub = subscription ?? await EnsureDefaultSubscriptionAsync();

            var mi = new MonitoredItem(sub.DefaultItem)
            {
                StartNodeId = nodeId,
                AttributeId = Attributes.EventNotifier,
                MonitoringMode = MonitoringMode.Reporting,
                QueueSize = 0,
                DiscardOldest = true
            };

            mi.Notification += (m, e) => handler(m, e);
            sub.AddItem(mi);
            await sub.ApplyChangesAsync();

            _monitored[(int)mi.ClientHandle] = mi;
            return mi;
        }

        public async Task SubscribeDataChangeAsync(IDataChangeHandler handler, IEnumerable<NodeId> nodes)
        {
            var session = _client.Session ?? throw new InvalidOperationException("No session");
            var sub = await EnsureDefaultSubscriptionAsync();
            var items = new List<MonitoredItem>();
            foreach (var nodeId in nodes)
            {
                var mi = new MonitoredItem(sub.DefaultItem)
                {
                    StartNodeId = nodeId,
                    AttributeId = Attributes.Value,
                    SamplingInterval = 0,
                    QueueSize = 1,
                    DiscardOldest = true
                };
                mi.Notification += async (m, e) =>
                {
                    if (_handlers.TryGetValue((int)mi.ClientHandle, out var pair))
                    {
                        try
                        {
                            var dv = await session.ReadValueAsync(nodeId);
                            pair.handler.DataChangeNotification(nodeId, dv, e);
                        }
                        catch { }
                    }
                };

                sub.AddItem(mi);
                items.Add(mi);
                _monitored[(int)mi.ClientHandle] = mi;
                _handlers[(int)mi.ClientHandle] = (handler, nodeId);
            }
            // apply changes once for all items (batch mode) with retry
            if (items.Count > 0)
            {
                await ApplySubscriptionChangesWithRetryAsync(sub);
            }
        }

        private async Task ApplySubscriptionChangesWithRetryAsync(Subscription sub, int maxAttempts = 3)
        {
            if (sub == null) throw new ArgumentNullException(nameof(sub));
            var attempt = 0;
            while (true)
            {
                attempt++;
                try
                {
                    // Ensure session is present and connected before applying subscription changes
                    var sess = _client.Session as Session;
                    var sessId = sess?.SessionId?.ToString() ?? "(no-session)";
                    UAClient.Common.Log.Debug($"SubscriptionManager: ApplyChanges attempt #{attempt} for SubscriptionId={sub.Id}, Session={sessId}");

                    var waited = 0;
                    var maxWaitMs = 2000;
                    var stepMs = 100;
                    while ((sess == null || !sess.Connected) && waited < maxWaitMs)
                    {
                        try { await Task.Delay(stepMs); } catch { }
                        waited += stepMs;
                        sess = _client.Session as Session;
                        if (sess != null && sess.Connected) break;
                    }

                    if (sess == null || !sess.Connected)
                    {
                        UAClient.Common.Log.Warn($"SubscriptionManager: Session not available/connected before ApplyChanges (SubscriptionId={sub.Id}). Will trigger retry logic.");
                        throw new InvalidOperationException("No session or session not connected");
                    }

                    await sub.ApplyChangesAsync();
                    return;
                }
                catch (Exception ex)
                {
                    // If session vanished or channel closed, attempt to recover by reconnecting the client
                    var sess = _client.Session as Session;
                    var sessId = sess?.SessionId?.ToString() ?? "(no-session)";

                    if (attempt >= maxAttempts)
                    {
                        UAClient.Common.Log.Warn($"SubscriptionManager: ApplyChanges failed after {attempt} attempts: {ex.Message} (SubscriptionId={sub.Id}, Session={sessId})");
                        throw;
                    }

                    UAClient.Common.Log.Debug($"SubscriptionManager: ApplyChanges attempt #{attempt} failed: {ex.Message} - attempting recovery... (SubscriptionId={sub.Id}, Session={sessId})");

                    // short backoff before recovery attempt
                    try { await Task.Delay(200 * attempt); } catch { }

                    // Recovery strategy:
                    // - If there's no session, wait briefly for an external reconnect to happen (do not force reconnect here),
                    //   because forcing a reconnect from multiple concurrent subscription setups can create races.
                    // - If session exists but exception indicates a closed channel, attempt a single reconnect (Disconnect+Connect)
                    var session = _client.Session;
                    if (session == null || !session.Connected)
                    {
                        UAClient.Common.Log.Info("SubscriptionManager: session not connected; waiting briefly for session to recover...");
                        // wait up to a short timeout for the session to come back
                        var waited = 0;
                        var maxWaitMs = 5000;
                        var stepMs = 200;
                        while (waited < maxWaitMs)
                        {
                            try { await Task.Delay(stepMs); } catch { }
                            waited += stepMs;
                            session = _client.Session;
                            if (session != null && session.Connected) break;
                        }
                        if (session == null || !session.Connected)
                        {
                            UAClient.Common.Log.Debug($"SubscriptionManager: session still not connected after wait (SubscriptionId={sub.Id}); will retry attempts but will not force reconnect here.");
                        }
                        else
                        {
                            UAClient.Common.Log.Debug($"SubscriptionManager: session recovered; retrying ApplyChanges (SubscriptionId={sub.Id}).");
                        }
                    }
                    else
                    {
                        // session exists; if the error indicates the channel closed, try a single reconnect to recover
                        if (ex.Message != null && ex.Message.IndexOf("Channel has been closed", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            // The channel was closed. Do NOT perform Connect/Disconnect here when the SDK's
                            // SessionReconnectHandler is in use. Instead wait for the session to be restored
                            // by the higher-level reconnect handler (RemoteServer/SDK). This avoids racing
                            // multiple concurrent reconnect attempts and creating short-lived sessions.

                            UAClient.Common.Log.Info($"SubscriptionManager: detected 'Channel has been closed' for SubscriptionId={sub.Id}; waiting for SDK-managed reconnect to recover (Session={sessId})");

                            // Wait for a reasonable time for the session to recover before retrying.
                            // Use an exponential backoff-like wait but do not call ConnectAsync here.
                            var recoveryWaitMs = 0;
                            var maxRecoveryWaitMs = 15000; // wait up to 15s for SDK to restore session
                            var stepMs = 250;
                            while (recoveryWaitMs < maxRecoveryWaitMs)
                            {
                                try { await Task.Delay(stepMs); } catch { }
                                recoveryWaitMs += stepMs;
                                session = _client.Session;
                                if (session != null && session.Connected)
                                {
                                    UAClient.Common.Log.Debug($"SubscriptionManager: detected recovered session (Session={session.SessionId}) for SubscriptionId={sub.Id}; will retry ApplyChanges");
                                    break;
                                }
                            }

                            if (session == null || !session.Connected)
                            {
                                UAClient.Common.Log.Debug($"SubscriptionManager: session did not recover within {maxRecoveryWaitMs}ms for SubscriptionId={sub.Id}; will continue retry attempts but will not call Connect here.");
                            }
                        }
                    }
                }
            }
        }

        public async Task RemoveMonitoredItemAsync(MonitoredItem item)
        {
            try
            {
                var sub = item.Subscription;
                sub?.RemoveItem(item);
                if (sub != null) await sub.ApplyChangesAsync();
                _monitored.TryRemove((int)item.ClientHandle, out _);
                _rawHandlers.TryRemove((int)item.ClientHandle, out _);
                _handlers.TryRemove((int)item.ClientHandle, out _);
            }
            catch { }
        }

        // Rebind all monitored items and subscriptions to the current client session.
        // This will delete existing subscriptions and recreate monitored items under the new session.
        public async Task RebindSubscriptionsAsync()
        {
            await _reconnectLock.WaitAsync();
            try
            {
                UAClient.Common.Log.Info("SubscriptionManager: Rebinding subscriptions to new session...");

                // Capture metadata for all monitored items so we can recreate them after deleting subscriptions
                var specs = new List<(NodeId nodeId, int attributeId, int samplingInterval, Action<MonitoredItem, MonitoredItemNotificationEventArgs>? actionHandler, IDataChangeHandler? dataHandler)>();
                foreach (var kv in _monitored)
                {
                    try
                    {
                        var clientHandle = kv.Key;
                        var mi = kv.Value;
                        var nodeId = mi.StartNodeId as NodeId;
                        if (nodeId == null) continue;
                        Action<MonitoredItem, MonitoredItemNotificationEventArgs>? raw = null;
                        if (_rawHandlers.TryGetValue(clientHandle, out var act)) raw = act;
                        IDataChangeHandler? dh = null;
                        if (_handlers.TryGetValue(clientHandle, out var pair)) dh = pair.handler;

                        specs.Add((nodeId, (int)mi.AttributeId, (int)mi.SamplingInterval, raw, dh));
                    }
                    catch { }
                }

                // delete existing subscriptions
                foreach (var s in _subscriptions.ToArray())
                {
                    try { await s.DeleteAsync(true); } catch { }
                }
                _subscriptions.Clear();

                // clear tracked monitored items and handlers; we'll recreate them
                _monitored.Clear();
                _rawHandlers.Clear();
                _handlers.Clear();
                _defaultSubscription = null;

                // Recreate monitored items from captured specs
                foreach (var spec in specs)
                {
                    try
                    {
                        if (spec.actionHandler != null)
                        {
                            await AddMonitoredItemAsync(spec.nodeId, spec.actionHandler, null, (uint)spec.samplingInterval);
                        }
                        else if (spec.dataHandler != null)
                        {
                            await SubscribeDataChangeAsync(spec.dataHandler, new[] { spec.nodeId });
                        }
                        else
                        {
                            // fallback: create a simple monitored item that ignores notifications
                            await AddMonitoredItemAsync(spec.nodeId, (m, e) => { }, null, (uint)spec.samplingInterval);
                        }
                    }
                    catch (Exception ex)
                    {
                        UAClient.Common.Log.Warn($"SubscriptionManager: failed to recreate monitored item for {spec.nodeId}: {ex.Message}");
                    }
                }

                UAClient.Common.Log.Info("SubscriptionManager: Rebind complete");
            }
            finally
            {
                _reconnectLock.Release();
            }
        }
    }
}
