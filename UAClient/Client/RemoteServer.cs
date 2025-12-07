using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using UAClient.Common;

namespace UAClient.Client
{
    public enum RemoteServerStatus
    {
        Disconnected,
        Connecting,
        Browsing,
        Connected
    }

    public interface IRemoteServerSubscriber
    {
        void OnConnectionLost();
        void OnConnectionEstablished();
        void OnServerTimeUpdate(DateTime time);
        void OnStatusChange(RemoteServerStatus status);
    }

    public class RemoteServer : IDisposable
    {
        private readonly UaClient _client;
        private readonly List<IRemoteServerSubscriber> _subscribers = new();
        private readonly SubscriptionManager _subscriptionManager;
        private readonly Dictionary<string, RemoteModule> _modules = new();
        // global components discovered across modules (key = "ModuleName:ComponentName")
        private readonly Dictionary<string, RemoteComponent> _components = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _namespaceMap = new();
        private CancellationTokenSource? _cts;
        private Task? _autoReconnectTask;
        private bool _firstConnection = true;

        public RemoteServer(UaClient client)
        {
            _client = client;
            _subscriptionManager = new SubscriptionManager(_client);
        }

        // Expose the internal SubscriptionManager so callers can reuse the global manager
        public SubscriptionManager SubscriptionManager => _subscriptionManager;
        public IReadOnlyDictionary<string, RemoteComponent> Components => _components;
        public IReadOnlyDictionary<string, RemoteModule> Modules => _modules;
        public IReadOnlyDictionary<string, int> NamespaceMap => _namespaceMap;
        public RemoteServerStatus Status { get; private set; } = RemoteServerStatus.Disconnected;

        public void AddSubscriber(IRemoteServerSubscriber s) => _subscribers.Add(s);
        public void RemoveSubscriber(IRemoteServerSubscriber s) => _subscribers.Remove(s);

        private void SetStatus(RemoteServerStatus s)
        {
            Status = s;
            foreach (var sub in _subscribers) sub.OnStatusChange(s);
        }

        public async Task ConnectAsync(double timeoutSeconds = 10, double reconnectTryDelay = 5, int? maxRetries = 3)
        {
            SetStatus(RemoteServerStatus.Connecting);
            await _client.ConnectAsync((uint)(timeoutSeconds * 1000));

            // build namespace map via session NamespaceUris
            var session = _client.Session ?? throw new InvalidOperationException("No session after connect");
            _namespaceMap.Clear();
            try
            {
                for (ushort i = 0; i < session.NamespaceUris.Count; i++)
                {
                    var uri = session.NamespaceUris.GetString(i);
                    if (uri != null && !_namespaceMap.ContainsKey(uri))
                        _namespaceMap[uri] = i;
                }
            }
            catch { }

            SetStatus(RemoteServerStatus.Browsing);
            UAClient.Common.Log.Info($"Session connected. Namespace count={session.NamespaceUris.Count}");

            _modules.Clear();

            // init subscription manager - not much to do because it's constructed with client
            // iterate modules/machines (fast discovery, no subscriptions)
            await IterateMachinesAsync(session);

            // discover components heuristically (no heavy subscriptions)
            try { await DiscoverComponentsAsync(session); } catch { }

            // perform expensive subscription setup in bounded parallelism
            try { await SetupAllSubscriptionsAsync(maxParallel: 6); } catch { }

            // setup Server time subscription (NodeId 2258)
            try
            {
                var serverTimeNode = new NodeId(2258, 0);
                await _subscriptionManager.AddMonitoredItemAsync(serverTimeNode, (m, e) =>
                {
                    // server time notifications are ignored in this simple implementation
                });
            }
            catch { }

            _firstConnection = false;
            SetStatus(RemoteServerStatus.Connected);

            // start auto reconnect checker
            _cts = new CancellationTokenSource();
            _autoReconnectTask = Task.Run(() => AutoReconnectLoop(reconnectTryDelay, _cts.Token));
        }

        private async Task IterateMachinesAsync(Session session)
        {
            // Browse Objects folder and find either Machines (v4) or ModuleSet (v3)
            var browser = new Browser(session)
            {
                BrowseDirection = BrowseDirection.Forward,
                ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
            };
            ReferenceDescriptionCollection refs = await browser.BrowseAsync(ObjectIds.ObjectsFolder);
            if (refs == null) return;

            // find candidates
            ReferenceDescription? moduleSetRef = null;
            foreach (var r in refs)
            {
                var bname = r.BrowseName;
                var ns = session.NamespaceUris.GetString(bname.NamespaceIndex);
                if (ns == NamespaceUris.NS_MACHINERY_URI && bname.Name == "Machines")
                {
                    moduleSetRef = r; break;
                }
                if (ns == NamespaceUris.NS_SFM_V3_URI && bname.Name == "ModuleSet")
                {
                    moduleSetRef = r; break;
                }
            }

            if (moduleSetRef == null)
            {
                // fallback: try to find any child that contains "Module" or "Machine"
                foreach (var r in refs)
                {
                    if (r.DisplayName.Text?.Contains("Module") == true || r.DisplayName.Text?.Contains("Machine") == true)
                    {
                        moduleSetRef = r; break;
                    }
                }
            }

            if (moduleSetRef == null) return;

            // get children of moduleSetRef
            var expanded = moduleSetRef.NodeId as ExpandedNodeId ?? new ExpandedNodeId(moduleSetRef.NodeId);
            var moduleSetNodeId = UaHelpers.ToNodeId(expanded, session);
            ReferenceDescriptionCollection children = await new Browser(session){BrowseDirection = BrowseDirection.Forward}.BrowseAsync(moduleSetNodeId);
            if (children == null) return;

            // Process module candidates in parallel with bounded concurrency to speed up discovery
            var maxParallelModules = 6;
            var sem = new System.Threading.SemaphoreSlim(maxParallelModules);
            var moduleTasks = new System.Collections.Generic.List<System.Threading.Tasks.Task>();
            foreach (var child in children)
            {
                try
                {
                    var expandedChild = child.NodeId as ExpandedNodeId ?? new ExpandedNodeId(child.NodeId);
                    var childNodeId = UaHelpers.ToNodeId(expandedChild, session);
                    var name = child.DisplayName.Text ?? childNodeId.ToString();

                    // Only treat nodes with HasTypeDefinition = MachineType as modules
                    RemoteModule? module = null;
                    try
                    {
                        // Browse HasTypeDefinition of the child
                        var typeBrowser = new Browser(session)
                        {
                            BrowseDirection = BrowseDirection.Forward,
                            ReferenceTypeId = ReferenceTypeIds.HasTypeDefinition,
                            IncludeSubtypes = true,
                            NodeClassMask = 0
                        };
                        ReferenceDescriptionCollection typeRefs = await typeBrowser.BrowseAsync(childNodeId);
                        if (typeRefs == null || typeRefs.Count == 0)
                        {
                            UAClient.Common.Log.Debug($"Skipping candidate '{name}' - no TypeDefinition found (node={childNodeId})");
                            continue;
                        }
                        var expandedType = typeRefs[0].NodeId as ExpandedNodeId ?? new ExpandedNodeId(typeRefs[0].NodeId);
                        var typeNodeId = UaHelpers.ToNodeId(expandedType, session);

                        // Read BrowseName of the type node
                        var readValueIds = new ReadValueIdCollection { new ReadValueId { NodeId = typeNodeId, AttributeId = Attributes.BrowseName } };
                        DataValueCollection results;
                        DiagnosticInfoCollection diagnosticInfos;
                        session.Read(null, 0, TimestampsToReturn.Neither, readValueIds, out results, out diagnosticInfos);
                        string? typeBrowseName = null;
                        if (results != null && results.Count > 0 && results[0].Value != null)
                        {
                            var qn = results[0].Value as QualifiedName;
                            typeBrowseName = qn?.Name;
                        }

                        // Accept nodes whose TypeDefinition name explicitly equals 'MachineType'
                        // but also accept types that contain the word 'Machine' (some servers use vendor-specific names)
                        if (string.IsNullOrEmpty(typeBrowseName) || typeBrowseName.IndexOf("Machine", StringComparison.OrdinalIgnoreCase) < 0)
                        {
                            UAClient.Common.Log.Debug($"Skipping candidate '{name}' - TypeDefinition is '{typeBrowseName ?? "(unknown)"}'");
                            continue;
                        }

                        UAClient.Common.Log.Info($"Found module candidate: {name} (node={childNodeId}) with TypeDefinition={typeBrowseName}");
                        module = new RemoteModule(name, childNodeId, _client, this);
                        _modules[name] = module;
                    }
                    catch (Exception ex)
                    {
                        UAClient.Common.Log.Warn($"Error inspecting candidate '{name}': {ex.Message}");
                        continue;
                    }

                    // start module setup in parallel, but limit number of concurrent setups
                    moduleTasks.Add(System.Threading.Tasks.Task.Run(async () =>
                    {
                        await sem.WaitAsync();
                        try
                        {
                                try
                                {
                                    // During initial server browse, avoid creating subscriptions to speed up discovery.
                                    await module.SetupSubscriptionsAsync(_subscriptionManager, false);
                                    // Resolve resource NodeIds (CarrierType/ProductType) for slots so client can reference resources by NodeId
                                    try { await module.ResolveResourceNodeIdsAsync(); } catch { }
                                    UAClient.Common.Log.Info($"Module '{name}' setup completed.");
                                }
                            catch (Exception ex)
                            {
                                UAClient.Common.Log.Warn($"Module '{name}' setup failed: {ex.Message}");
                            }
                        }
                        finally
                        {
                            sem.Release();
                        }
                    }));
                }
                catch { }
            }

            try { await System.Threading.Tasks.Task.WhenAll(moduleTasks); } catch { }
        }

        private async Task DiscoverComponentsAsync(Session session)
        {
            // For each discovered module, inspect its Methods map for potential components
            foreach (var kv in _modules)
            {
                var moduleName = kv.Key;
                var module = kv.Value;
                // iterate module.Methods entries
                foreach (var m in module.Methods)
                {
                    try
                    {
                        var compName = m.Key;
                        var callable = m.Value;
                        // Heuristic: browse the callable node for typical component children
                        bool looksLikeComponent = false;
                        try
                        {
                            var browser = new Browser(session)
                            {
                                BrowseDirection = BrowseDirection.Forward,
                                ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                                NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                            };
                            ReferenceDescriptionCollection refs = null;
                            try { refs = await browser.BrowseAsync(callable.BaseNodeId); } catch { }
                            if (refs != null)
                            {
                                foreach (var r in refs)
                                {
                                    var nm = r.DisplayName?.Text ?? r.BrowseName?.Name ?? string.Empty;
                                    if (string.Equals(nm, "Monitoring", StringComparison.OrdinalIgnoreCase)
                                        || string.Equals(nm, "ParameterSet", StringComparison.OrdinalIgnoreCase)
                                        || string.Equals(nm, "Attributes", StringComparison.OrdinalIgnoreCase)
                                        || string.Equals(nm, "SkillSet", StringComparison.OrdinalIgnoreCase)
                                        || string.Equals(nm, "Components", StringComparison.OrdinalIgnoreCase)
                                        || string.Equals(nm, "MethodSet", StringComparison.OrdinalIgnoreCase))
                                    {
                                        looksLikeComponent = true;
                                        break;
                                    }
                                }
                            }
                        }
                        catch { }

                        if (!looksLikeComponent) continue;

                        // Create component and populate subscriptions using the global subscription manager
                        var comp = new RemoteComponent(compName, callable.BaseNodeId, _client, this);
                        try { await comp.SetupSubscriptionsAsync(_subscriptionManager, true); } catch (Exception ex) { UAClient.Common.Log.Warn($"Component setup for {moduleName}:{compName} failed: {ex.Message}"); }
                        var key = $"{moduleName}:{compName}";
                        _components[key] = comp;
                        UAClient.Common.Log.Info($"Discovered component: {key}");
                    }
                    catch { }
                }
            }
        }

        private async Task AutoReconnectLoop(double delaySeconds, CancellationToken token)
        {
            var session = _client.Session;
            while (!token.IsCancellationRequested)
            {
                try
                {
                    session = _client.Session;
                    if (session == null || !session.Connected)
                    {
                        // attempt reconnect
                        foreach (var sub in _subscribers) sub.OnConnectionLost();
                        SetStatus(RemoteServerStatus.Disconnected);
                        bool connected = false;
                        while (!connected && !token.IsCancellationRequested)
                        {
                            try
                            {
                                await _client.ConnectAsync();
                                connected = true;
                                foreach (var sub in _subscribers) sub.OnConnectionEstablished();
                                SetStatus(RemoteServerStatus.Connected);
                                // re-browse on reconnect
                                await IterateMachinesAsync(_client.Session!);
                            }
                            catch
                            {
                                await Task.Delay(TimeSpan.FromSeconds(delaySeconds), token);
                            }
                        }
                    }
                }
                catch { }

                await Task.Delay(TimeSpan.FromSeconds(0.5), token);
            }
        }

        // After discovery, perform subscription creation for modules/components/skills in parallel with bounded concurrency
        private async Task SetupAllSubscriptionsAsync(int maxParallel = 6)
        {
            var sem = new System.Threading.SemaphoreSlim(maxParallel);
            var tasks = new List<Task>();

            // Modules: subscribe core monitoring variables only
            foreach (var module in _modules.Values)
            {
                tasks.Add(Task.Run(async () =>
                {
                    await sem.WaitAsync();
                    try
                    {
                        try { await module.SubscribeCoreAsync(_subscriptionManager); }
                        catch (Exception ex) { UAClient.Common.Log.Warn($"Module core subscription failed: {ex.Message}"); }
                    }
                    finally { sem.Release(); }
                }));
            }

            // Components: subscribe core monitoring variables only
            foreach (var comp in _components.Values)
            {
                tasks.Add(Task.Run(async () =>
                {
                    await sem.WaitAsync();
                    try
                    {
                        try { await comp.SubscribeCoreAsync(_subscriptionManager); }
                        catch (Exception ex) { UAClient.Common.Log.Warn($"Component core subscription failed: {ex.Message}"); }
                    }
                    finally { sem.Release(); }
                }));
            }

            try { await Task.WhenAll(tasks); } catch { }
        }

        public void Dispose()
        {
            try { _cts?.Cancel(); } catch { }
            _subscriptionManager.Dispose();
        }
    }
}
