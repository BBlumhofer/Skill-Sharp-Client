using System;
using System.Collections.Generic;
using Opc.Ua;
using Opc.Ua.Client;
using System.Linq;
using System.Threading;

namespace UAClient.Client
{
    public class RemoteModule
    {
        // If true, allow falling back to the first discovered Lock object when no
        // explicit Lock candidate was found. Default is false (strict mode).
        public bool AllowLockFallback { get; set; } = false;

        public string Name { get; }
        public NodeId BaseNodeId { get; }
        public IDictionary<string, BaseRemoteCallable> Methods { get; } = new Dictionary<string, BaseRemoteCallable>();
        public RemoteLock? Lock { get; private set; }
        
        public bool IsLockedByUs { get { lock (this) { return _isLockedByUs; } } private set { lock (this) { _isLockedByUs = value; } }}
        private bool _isLockedByUs = false;
        public IDictionary<string, RemotePort> Ports { get; } = new Dictionary<string, RemotePort>();
        public List<string> Neighbors{get; private set;} = new List<string>();
        /// <summary>
        /// Liefert ein Mapping von Port-Name auf Partner-RFID-Tag für alle Ports dieses Moduls,
        /// aber nur für Ports, deren `Closed`-Eigenschaft true ist. Leere oder null-Tags werden
        /// nicht in das Ergebnis aufgenommen.
        /// </summary>

        public IDictionary<string, RemoteStorage> Storages { get; } = new Dictionary<string, RemoteStorage>();
        public IDictionary<string, RemoteComponent> Components { get; } = new Dictionary<string, RemoteComponent>(StringComparer.OrdinalIgnoreCase);
        public IDictionary<string, RemoteSkill> SkillSet { get; } = new Dictionary<string, RemoteSkill>(StringComparer.OrdinalIgnoreCase);
        public IDictionary<string, RemoteVariable> Monitoring { get; } = new Dictionary<string, RemoteVariable>(StringComparer.OrdinalIgnoreCase);
        private IDictionary<NodeId, RemoteVariable> _monitoring_nodes = new Dictionary<NodeId, RemoteVariable>();
        private UaClient _client;
        private RemoteServer _remoteServer;

        public RemoteModule(string name, NodeId baseNodeId, UaClient client, RemoteServer remoteServer)
        {
            Name = name;
            BaseNodeId = baseNodeId;
            _client = client;
            _remoteServer = remoteServer;
        }

        // Convenience property exposing whether module is ready (locked, coupled, startup skill running)
        public bool IsReady
        {
            get { lock (this) { return _isReady; } }
            private set { lock (this) { _isReady = value; } }
        }
        private bool _isReady = false;

        // Enable auto-relock: re-lock module when it becomes unlocked. Uses RemoteServer.SubscriptionManager when available.
        private bool _autoRelockEnabled = false;
        private readonly List<MonitoredItem> _autoRelockMonitoredItems = new List<MonitoredItem>();

        public IDictionary<string, string> GetClosedPortsPartnerRfidTags()
        {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                foreach (var kv in Ports)
                {
                    try
                    {
                        var portName = kv.Key;
                        var port = kv.Value;
                        if (port == null) continue;
                        if (!port.Closed) continue;
                        var tag = port.PartnerRfidTag;
                        if (!string.IsNullOrEmpty(tag)) map[portName] = tag!;
                    }
                    catch { /* ignore per-port errors */ }
                }
            }
            catch { }
            return map;
        }


        public async Task EnableAutoRelockAsync()
        {
            if (_autoRelockEnabled) return;
            _autoRelockEnabled = true;
            try
            {
                var subMgr = _remoteServer?.SubscriptionManager;
                if (subMgr == null) return;
                var session = _client?.Session ?? throw new InvalidOperationException("No session");

                // subscribe to lock state variable(s)
                if (Lock != null && Lock.CurrentState != null)
                {
                    // subscribe to Lock.CurrentState (quiet - avoid noisy debug output)
                    var mi = await subMgr.AddMonitoredItemAsync(Lock.CurrentState.NodeId, (m, e) => { _ = Task.Run(async () => await OnLockStateChangedAsync()); });
                    lock (_autoRelockMonitoredItems) { _autoRelockMonitoredItems.Add(mi); }
                }
                else if (Lock != null && Lock.Variables.TryGetValue("Locked", out var lockedVar))
                {
                    // subscribe to Lock.Locked (quiet - avoid noisy debug output)
                    var mi = await subMgr.AddMonitoredItemAsync(lockedVar.NodeId, (m, e) => { _ = Task.Run(async () => await OnLockStateChangedAsync()); });
                    lock (_autoRelockMonitoredItems) { _autoRelockMonitoredItems.Add(mi); }
                }

                // also subscribe to owner changes so we can detect foreign ownership quickly
                try
                {
                    if (Lock != null)
                    {
                        if (Lock.Descriptor != null && Lock.Descriptor.LockingClient != null)
                        {
                            UAClient.Common.Log.Debug($"EnableAutoRelockAsync: subscribing to Descriptor.LockingClient node={Lock.Descriptor.LockingClient}");
                            var mi2 = await subMgr.AddMonitoredItemAsync(Lock.Descriptor.LockingClient, (m, e) => { _ = Task.Run(async () => await OnLockStateChangedAsync()); });
                            lock (_autoRelockMonitoredItems) { _autoRelockMonitoredItems.Add(mi2); }
                        }
                        if (Lock.LockingClient != null)
                        {
                            UAClient.Common.Log.Debug($"EnableAutoRelockAsync: subscribing to Lock.LockingClient node={Lock.LockingClient.NodeId}");
                            var mi3 = await subMgr.AddMonitoredItemAsync(Lock.LockingClient.NodeId, (m, e) => { _ = Task.Run(async () => await OnLockStateChangedAsync()); });
                            lock (_autoRelockMonitoredItems) { _autoRelockMonitoredItems.Add(mi3); }
                        }
                        if (Lock.LockingUser != null)
                        {
                            UAClient.Common.Log.Debug($"EnableAutoRelockAsync: subscribing to Lock.LockingUser node={Lock.LockingUser.NodeId}");
                            var mi4 = await subMgr.AddMonitoredItemAsync(Lock.LockingUser.NodeId, (m, e) => { _ = Task.Run(async () => await OnLockStateChangedAsync()); });
                            lock (_autoRelockMonitoredItems) { _autoRelockMonitoredItems.Add(mi4); }
                        }
                    }
                }
                catch (Exception ex) { UAClient.Common.Log.Debug($"EnableAutoRelockAsync: owner subscription failed: {ex.Message}"); }

                UAClient.Common.Log.Debug($"EnableAutoRelockAsync: auto-relock enabled, monitored items={_autoRelockMonitoredItems.Count}");
            }
            catch (Exception ex) { UAClient.Common.Log.Debug($"EnableAutoRelockAsync: failed to enable auto-relock: {ex.Message}"); }
        }

        private async Task OnLockStateChangedAsync()
        {
                try
                {
                    var session = _client?.Session ?? throw new InvalidOperationException("No session");
                    if (Lock == null) return;

                    // Check lock state (minimal logging to avoid console spam)
                    var locked = await Lock.IsLockedAsync(session);

                // If locked, check owner. If owned by someone else, do not attempt relock.
                if (locked.HasValue && locked.Value)
                {
                        try
                        {
                            var owner = await Lock.GetLockOwnerAsync(session);
                            var ourId = _client?.Configuration?.ApplicationName ?? string.Empty;
                        if (!string.IsNullOrEmpty(owner) && !string.IsNullOrEmpty(ourId))
                        {
                            // if owner contains our id, assume we hold the lock
                            if (owner.IndexOf(ourId, StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                UAClient.Common.Log.Debug($"RemoteModule '{Name}': lock is held by us (owner={owner})");
                                // nothing to do
                                await EvaluateReadyAsync();
                                return;
                            }
                            else
                            {
                                UAClient.Common.Log.Debug($"RemoteModule '{Name}': lock is held by other client (owner={owner}), will not relock");
                                await EvaluateReadyAsync();
                                return;
                            }
                        }
                        // if owner unknown, just avoid aggressive relock when it's locked by anyone
                        UAClient.Common.Log.Debug($"RemoteModule '{Name}': lock is held (owner unknown), skipping relock");
                        await EvaluateReadyAsync();
                        return;
                    }
                        catch (Exception ex)
                        {
                            // reading owner failed - log at Warn level
                            UAClient.Common.Log.Warn($"OnLockStateChangedAsync: failed to read owner: {ex.Message}");
                            await EvaluateReadyAsync();
                            return;
                        }
                }

                // not locked -> attempt relock (best-effort)
                    try
                    {
                        UAClient.Common.Log.Debug($"RemoteModule '{Name}': lock appears free, attempting relock");
                        var before = DateTime.UtcNow;
                        var relockResult = await LockAsync(session);
                        // relock result processed; avoid verbose timing debug by default
                    }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': auto-relock attempt failed: {ex.Message}");
                }
                // re-evaluate readiness
                await EvaluateReadyAsync();
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Debug($"OnLockStateChangedAsync: unexpected error: {ex.Message}");
            }
        }

        public async Task DisableAutoRelockAsync()
        {
            _autoRelockEnabled = false;
            try
            {
                var subMgr = _remoteServer?.SubscriptionManager;
                if (subMgr == null) return;
                lock (_autoRelockMonitoredItems)
                {
                    foreach (var mi in _autoRelockMonitoredItems)
                    {
                        try { _ = subMgr.RemoveMonitoredItemAsync(mi); } catch { }
                    }
                    _autoRelockMonitoredItems.Clear();
                }
            }
            catch { }
        }

        private async System.Threading.Tasks.Task<NodeId?> GetTypeDefinitionAsync(Session session, NodeId nodeId)
        {
            try
            {
                var browser = new Browser(session)
                {
                    BrowseDirection = BrowseDirection.Forward,
                    ReferenceTypeId = ReferenceTypeIds.HasTypeDefinition,
                    IncludeSubtypes = true,
                    NodeClassMask = 0 // any
                };
                var refs = await browser.BrowseAsync(nodeId);
                if (refs != null && refs.Count > 0)
                {
                    var expanded = refs[0].NodeId as ExpandedNodeId ?? new ExpandedNodeId(refs[0].NodeId);
                    return UaHelpers.ToNodeId(expanded, session);
                }
            }
            catch { }
            return null;
        }

        private async System.Threading.Tasks.Task<string?> GetBrowseNameAsync(Session session, NodeId nodeId)
        {
            try
            {
                var readValueIds = new ReadValueIdCollection { new ReadValueId { NodeId = nodeId, AttributeId = Attributes.BrowseName } };
                DataValueCollection results;
                DiagnosticInfoCollection diagnosticInfos;
                session.Read(null, 0, TimestampsToReturn.Neither, readValueIds, out results, out diagnosticInfos);
                if (results != null && results.Count > 0 && results[0].Value != null)
                {
                    var qn = results[0].Value as QualifiedName;
                    return qn?.Name;
                }
            }
            catch { }
            return null;
        }

        // Debug helper: recursively browse a node and log its children (limited depth).
        // Enhanced to read DisplayName and Variable values when available for better debugging.
        private async System.Threading.Tasks.Task BrowseAndLogNodeRecursive(Session session, NodeId nodeId, int depth, int maxDepth)
        {
            try
            {
                if (nodeId == null || nodeId.Equals(NodeId.Null)) return;
                var indent = new string(' ', depth * 2);

                // Read BrowseName and DisplayName attributes
                string bname = "(unknown)";
                string display = "(unknown)";
                try
                {
                    bname = await GetBrowseNameAsync(session, nodeId) ?? "(null)";
                }
                catch { }
                try
                {
                    var readDisplay = new ReadValueIdCollection { new ReadValueId { NodeId = nodeId, AttributeId = Attributes.DisplayName } };
                    DataValueCollection results;
                    DiagnosticInfoCollection diag;
                    session.Read(null, 0, TimestampsToReturn.Neither, readDisplay, out results, out diag);
                    if (results != null && results.Count > 0 && results[0].Value != null)
                    {
                        var dn = results[0].Value as LocalizedText;
                        display = dn?.Text ?? dn?.ToString() ?? "(null)";
                    }
                }
                catch { }

                UAClient.Common.Log.Debug($"{indent}BrowseNode: NodeId={nodeId} BrowseName={bname} DisplayName={display}");

                if (depth >= maxDepth) return;

                // Browse children (hierarchical)
                try
                {
                    var browser = new Browser(session)
                    {
                        BrowseDirection = BrowseDirection.Forward,
                        ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                        IncludeSubtypes = true,
                        NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                    };
                    var refs = await browser.BrowseAsync(nodeId);
                    if (refs != null)
                    {
                        foreach (var r in refs)
                        {
                            try
                            {
                                var expanded = r.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r.NodeId);
                                var childId = UaHelpers.ToNodeId(expanded, session);
                                var childDisplay = r.DisplayName?.Text ?? "";
                                var childBrowse = r.BrowseName?.Name ?? "";
                                var childClass = r.NodeClass.ToString();
                                UAClient.Common.Log.Debug($"{indent}- Child: BrowseName={childBrowse} DisplayName={childDisplay} NodeClass={childClass} NodeId={childId}");

                                // If it's a variable, try to read its value for debugging
                                if (childId != null && !childId.Equals(NodeId.Null) && r.NodeClass == NodeClass.Variable)
                                {
                                    try
                                    {
                                        var dv = await session.ReadValueAsync(childId, System.Threading.CancellationToken.None);
                                        var val = dv?.Value?.ToString() ?? "(null)";
                                        var datatype = dv?.Value != null ? dv.Value.GetType().ToString() : "(unknown)";
                                        UAClient.Common.Log.Debug($"{indent}  (Variable) Value={val} DataType={datatype}");
                                    }
                                    catch (Exception exVal)
                                    {
                                        UAClient.Common.Log.Debug($"{indent}  (Variable) read failed: {exVal.Message}");
                                    }
                                }

                                if (childId != null && !childId.Equals(NodeId.Null))
                                {
                                    // Recurse
                                    await BrowseAndLogNodeRecursive(session, childId, depth + 1, maxDepth);
                                }
                            }
                            catch (Exception exChild)
                            {
                                UAClient.Common.Log.Debug($"{indent}- Child browse failed: {exChild.Message}");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Debug($"{indent}BrowseAsync failed for {nodeId}: {ex.Message}");
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Debug($"BrowseAndLogNodeRecursive error for {nodeId}: {ex.Message}");
            }
        }

        private async System.Threading.Tasks.Task<object?> CreateRemoteObjectFromTypeDefinitionAsync(Session session, NodeId nodeId, string name)
        {
            // Define base types by BrowseName
            var machineryItemTypes = new System.Collections.Generic.HashSet<string>
            {
                "TopologyElementType",
                "ComponentType",
                "DeviceType",
                "MachineryItemType",
                "MachineType",
                "StorageSlotType",
                "GripperType",
                "ShuttleType",
                "SafetyType",
                "DoorType",
                "ConveyingAreaType",
                // Machinery specific
                "MachineryItemIdentificationType",
                "MachineIdentificationType",
                "MachineryComponentIdentificationType",
                "MachineryOperationCounterType",
                "MachineryLifetimeCounterType",
                "MachineComponentsType",
                "MachineryItemState_StateMachineType",
                "MachineryOperationModeStateMachineType",
                // IA specific
                "BasicStacklightType",
                "StacklightType",
                "CalibrationTargetType",
                "AcousticSignalType",
                "StackElementType"
            };
            var skillTypes = new System.Collections.Generic.HashSet<string>
            {
                "FiniteSkillType",
                "ContinuousSkillType"
            };

            // Check for Locks  
            NodeId type_def = await GetTypeDefinitionAsync(session, nodeId);
            // If this node is a LockingServicesType, create a single Lock object
            if (await IsTypeOrSubtypeOfAsync(session, nodeId, new System.Collections.Generic.HashSet<string> { "LockingServicesType" }))
            {
                var lockObj = new RemoteLock(name, nodeId);
                await RemoteVariableCollector.AddVariableNodesAsync(session, nodeId, lockObj.NodeMap, lockObj.Variables, true);
                // Populate descriptor and methods
                var desc = new RemoteLockDescriptor { BaseNodeId = nodeId };
                // Enumerate children for methods
                try
                {
                    var browserLock = new Browser(session)
                    {
                        BrowseDirection = BrowseDirection.Forward,
                        ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                        NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                    };
                    var refsLock = await browserLock.BrowseAsync(nodeId);
                    if (refsLock != null)
                    {
                        foreach (var rLock in refsLock)
                        {
                            try
                            {
                                if (rLock?.NodeId == null) continue;
                                var expandedLock = rLock.NodeId as ExpandedNodeId ?? new ExpandedNodeId(rLock.NodeId);
                                var mId = UaHelpers.ToNodeId(expandedLock, session);
                                if (mId == null) continue;
                                var mName = rLock.DisplayName?.Text ?? rLock.BrowseName?.Name ?? mId.ToString();
                                var rm = new RemoteMethod(mName, mId, _client);
                                lockObj.Methods[mName] = rm;
                                lockObj.MethodsNodeMap[mId] = rm;
                                // Map to descriptor and properties
                                if (mName.IndexOf("InitLock", StringComparison.OrdinalIgnoreCase) >= 0) { desc.InitLock = mId; lockObj.InitLock = rm; }
                                if (mName.IndexOf("BreakLock", StringComparison.OrdinalIgnoreCase) >= 0) { desc.BreakLock = mId; lockObj.BreakLock = rm; }
                                if (mName.IndexOf("ExitLock", StringComparison.OrdinalIgnoreCase) >= 0) { desc.ExitLock = mId; lockObj.ExitLock = rm; }
                                if (mName.IndexOf("RenewLock", StringComparison.OrdinalIgnoreCase) >= 0) { desc.RenewLock = mId; lockObj.RenewLock = rm; }
                                if (mName.IndexOf("Skill", StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    var rs = new RemoteSkill(mName, mId, _client, _remoteServer);
                                    lockObj.Methods[$"skill:{mName}"] = rs;
                                    lockObj.MethodsNodeMap[mId] = rs;
                                }
                            }
                            catch { }
                        }
                    }
                }
                catch { }
                // Map variables to descriptor
                foreach (var kv in lockObj.Variables)
                {
                    var key = kv.Key ?? "";
                    var nid = kv.Value.NodeId;
                    if (key.IndexOf("Locked", StringComparison.OrdinalIgnoreCase) >= 0) desc.Locked = nid;
                    if (key.IndexOf("LockingUser", StringComparison.OrdinalIgnoreCase) >= 0) desc.LockingUser = nid;
                    if (key.IndexOf("LockingClient", StringComparison.OrdinalIgnoreCase) >= 0) desc.LockingClient = nid;
                    if (key.IndexOf("RemainingLockTime", StringComparison.OrdinalIgnoreCase) >= 0) desc.RemainingLockTime = nid;
                    if (key.IndexOf("CurrentState", StringComparison.OrdinalIgnoreCase) >= 0 || key.IndexOf("State", StringComparison.OrdinalIgnoreCase) >= 0) desc.CurrentState = nid;
                }
                // Set convenience properties
                if (lockObj.Variables.TryGetValue("Locked", out var rvLocked)) lockObj.Locked = rvLocked;
                if (lockObj.Variables.TryGetValue("LockingUser", out var rvUser)) lockObj.LockingUser = rvUser;
                if (lockObj.Variables.TryGetValue("LockingClient", out var rvClient)) lockObj.LockingClient = rvClient;
                if (lockObj.Variables.TryGetValue("RemainingLockTime", out var rvRemaining)) lockObj.RemainingLockTime = rvRemaining;
                if (lockObj.Variables.TryGetValue("CurrentState", out var rvState)) lockObj.CurrentState = rvState;
                lockObj.Descriptor = desc;
                return lockObj;
            }
            // Check for Ports
            else if (await IsTypeOrSubtypeOfAsync(session, nodeId, new System.Collections.Generic.HashSet<string> { "PortType" }))
            {
                var port = new RemotePort(name, nodeId, _client, _remoteServer, this);
                await RemoteVariableCollector.AddVariableNodesAsync(session, nodeId, port.NodeMap, port.Variables, true);
                // Attach methods
                try
                {
                    var browser3 = new Browser(session)
                    {
                        BrowseDirection = BrowseDirection.Forward,
                        ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                        NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                    };
                    var refs3 = await browser3.BrowseAsync(nodeId);
                    if (refs3 != null)
                    {
                        foreach (var r3 in refs3)
                        {
                            try
                            {
                                if (r3?.NodeId == null) continue;
                                var expanded3 = r3.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r3.NodeId);
                                var methodId = UaHelpers.ToNodeId(expanded3, session);
                                if (methodId == null) continue;
                                var methodName = r3.DisplayName?.Text ?? r3.BrowseName?.Name ?? methodId.ToString();
                                var rm = new RemoteMethod(methodName, methodId, _client);
                                port.Methods[methodName] = rm;
                                port.MethodsNodeMap[methodId] = rm;
                                if (methodName.IndexOf("Skill", StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    var rs = new RemoteSkill(methodName, methodId, _client, _remoteServer);
                                    port.Methods[$"skill:{methodName}"] = rs;
                                    port.MethodsNodeMap[methodId] = rs;
                                }
                            }
                            catch { }
                        }
                    }
                }
                catch { }
                return port;
            }
            // Check for Storages
            else if (await IsTypeOrSubtypeOfAsync(session, nodeId, new System.Collections.Generic.HashSet<string> { "StorageType" }))
            {
                var storage = new RemoteStorage(name, nodeId);
                await RemoteVariableCollector.AddVariableNodesAsync(session, nodeId, storage.NodeMap, storage.Variables, true);
                // Attempt to discover StorageSlot children under the storage
                try
                {
                    var browser = new Browser(session)
                    {
                        BrowseDirection = BrowseDirection.Forward,
                        ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                        NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                    };
                    var refs = await browser.BrowseAsync(nodeId);
                    if (refs != null)
                    {
                        foreach (var r in refs)
                        {
                            try
                            {
                                if (r?.NodeId == null) continue;
                                var expanded = r.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r.NodeId);
                                var childId = UaHelpers.ToNodeId(expanded, session);
                                if (childId == null) continue;
                                var childName = r.DisplayName?.Text ?? r.BrowseName?.Name ?? childId.ToString();

                                // If the child is a StorageSlot (by browse name or type), create a RemoteStorageSlot
                                var isSlot = false;
                                try
                                {
                                    if (!string.IsNullOrEmpty(r.BrowseName?.Name) && r.BrowseName.Name.IndexOf("StorageSlot", StringComparison.OrdinalIgnoreCase) >= 0)
                                    {
                                        isSlot = true;
                                    }
                                    else
                                    {
                                        isSlot = await IsTypeOrSubtypeOfAsync(session, childId, new System.Collections.Generic.HashSet<string> { "StorageSlotType" });
                                    }
                                }
                                catch { }

                                if (isSlot)
                                {
                                    try
                                    {
                                        var slot = new RemoteStorageSlot(childName, childId);
                                        await RemoteVariableCollector.AddVariableNodesAsync(session, childId, slot.NodeMap, slot.Variables, false);
                                        // add to storage.Slots
                                        try { storage.Slots[childName] = slot; } catch { }
                                    }
                                    catch { }
                                }
                            }
                            catch { }
                        }
                    }
                }
                catch { }

                // Also check for a 'Components' child under the storage (slots may be placed there)
                try
                {
                    var browserC = new Browser(session)
                    {
                        BrowseDirection = BrowseDirection.Forward,
                        ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                        NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                    };
                    var topRefs = await browserC.BrowseAsync(nodeId);
                    if (topRefs != null)
                    {
                        NodeId? compContainerId = null;
                        foreach (var tr in topRefs)
                        {
                            try
                            {
                                var trName = tr?.DisplayName?.Text ?? tr?.BrowseName?.Name ?? string.Empty;
                                if (string.Equals(trName, "Components", StringComparison.OrdinalIgnoreCase))
                                {
                                    var expanded = tr.NodeId as ExpandedNodeId ?? new ExpandedNodeId(tr.NodeId);
                                    compContainerId = UaHelpers.ToNodeId(expanded, session);
                                    break;
                                }
                            }
                            catch { }
                        }

                        if (compContainerId != null)
                        {
                            var crefs = await browserC.BrowseAsync(compContainerId);
                            if (crefs != null)
                            {
                                foreach (var cr in crefs)
                                {
                                    try
                                    {
                                        if (cr?.NodeId == null) continue;
                                        var expanded = cr.NodeId as ExpandedNodeId ?? new ExpandedNodeId(cr.NodeId);
                                        var compId = UaHelpers.ToNodeId(expanded, session);
                                        if (compId == null) continue;
                                        var compName = cr.DisplayName?.Text ?? cr.BrowseName?.Name ?? compId.ToString();
                                        var isSlot2 = false;
                                        try { isSlot2 = await IsTypeOrSubtypeOfAsync(session, compId, new System.Collections.Generic.HashSet<string> { "StorageSlotType" }); } catch { }
                                        if (isSlot2)
                                        {
                                            try
                                            {
                                                var slot = new RemoteStorageSlot(compName, compId);
                                                await RemoteVariableCollector.AddVariableNodesAsync(session, compId, slot.NodeMap, slot.Variables, false);
                                                try { storage.Slots[compName] = slot; } catch { }
                                            }
                                            catch { }
                                        }
                                    }
                                    catch { }
                                }
                            }
                        }
                    }
                }
                catch { }

                return storage;
            }
            // Check for Machinery Items (Components)
            else if (await IsTypeOrSubtypeOfAsync(session, nodeId, machineryItemTypes))
            {
                var component = new RemoteComponent(name, nodeId, _client, _remoteServer);
                return component;
            }
            // Check for Skills
            else if (await IsTypeOrSubtypeOfAsync(session, nodeId, skillTypes))
            {
                var skill = new RemoteSkill(name, nodeId, _client, _remoteServer);
                return skill;
            }
            return null;
        }

        public async System.Threading.Tasks.Task DiscoverMethodsAsync()
        {
            var session = _client.Session;
            if (session == null) return;

            var visited = new System.Collections.Concurrent.ConcurrentDictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            var queue = new System.Collections.Concurrent.ConcurrentQueue<NodeId>();
            queue.Enqueue(BaseNodeId);
            var dictLock = new object();

            while (queue.TryDequeue(out var node))
            {
                if (node == null) continue;
                var nodeKey = node.ToString();
                if (!visited.TryAdd(nodeKey, true)) continue;

                var browser = new Browser(session)
                {
                    BrowseDirection = BrowseDirection.Forward,
                    ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                    NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                };
                ReferenceDescriptionCollection refs = null;
                try { refs = await browser.BrowseAsync(node); } catch { }
                if (refs == null) continue;

                var tasks = new System.Collections.Generic.List<System.Threading.Tasks.Task>();
                foreach (var r in refs)
                {
                    // process each child reference in parallel
                    tasks.Add(System.Threading.Tasks.Task.Run(async () =>
                    {
                        try
                        {
                            if (r?.NodeId == null) return;
                            var expanded = r.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r.NodeId);
                            var childId = UaHelpers.ToNodeId(expanded, session);
                            if (childId == null) return;
                            var name = r.DisplayName?.Text ?? childId.ToString() ?? "<unknown>";
                            var browseName = r.BrowseName?.Name ?? "";

                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': discovered child {name} (browse='{browseName}') of class {r.NodeClass}");

                            // Get the child's type definition browse name (if any)
                            string? childTypeBrowseName = null;
                            try
                            {
                                var typeDef = await GetTypeDefinitionAsync(session, childId);
                                if (typeDef != null) childTypeBrowseName = await GetBrowseNameAsync(session, typeDef);
                            }
                            catch { }

                            // If this is a Monitoring container (MonitoringType), collect variables into Monitoring map
                            if (!string.IsNullOrEmpty(childTypeBrowseName) && childTypeBrowseName.IndexOf("MonitoringType", StringComparison.OrdinalIgnoreCase) >= 0
                                || string.Equals(browseName, "Monitoring", StringComparison.OrdinalIgnoreCase))
                            {
                                try
                                {
                                    await RemoteVariableCollector.AddVariableNodesAsync(session, childId, _monitoring_nodes, Monitoring, false);
                                    UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Monitoring variables from '{name}' ({Monitoring.Count} vars)");
                                }
                                catch { }
                                return;
                            }

                            // Special handling: if this child is a MachineComponentsType (a container of components), enumerate its children and create components
                            if (!string.IsNullOrEmpty(childTypeBrowseName) && childTypeBrowseName.IndexOf("MachineComponentsType", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                try
                                {
                                    var browserCompContainer = new Browser(session)
                                    {
                                        BrowseDirection = BrowseDirection.Forward,
                                        ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                                        NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                                    };
                                    var compRefs = await browserCompContainer.BrowseAsync(childId);
                                    if (compRefs != null)
                                    {
                                        foreach (var cr in compRefs)
                                        {
                                            try
                                            {
                                                if (cr?.NodeId == null) continue;
                                                var exp = cr.NodeId as ExpandedNodeId ?? new ExpandedNodeId(cr.NodeId);
                                                var compId = UaHelpers.ToNodeId(exp, session);
                                                if (compId == null) continue;
                                                var compName = cr.DisplayName?.Text ?? cr.BrowseName?.Name ?? compId.ToString();
                                                var remoteObj = await CreateRemoteObjectFromTypeDefinitionAsync(session, compId, compName);
                                                if (remoteObj != null)
                                                {
                                                    lock (dictLock)
                                                    {
                                                        if (remoteObj is RemotePort port)
                                                        {
                                                            Ports[compName] = port;
                                                            // expose the same RemotePort instance in Components so both maps reference the same object
                                                            try
                                                            {
                                                                Components[compName] = port;
                                                            }
                                                            catch { }
                                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Port from MachineComponents '{compName}'");
                                                        }
                                                        else if (remoteObj is RemoteStorage storage)
                                                        {
                                                            Storages[compName] = storage;
                                                            try
                                                            {
                                                                var compWrapper = new RemoteComponent(compName, compId, _client, _remoteServer);
                                                                Components[compName] = compWrapper;
                                                            }
                                                            catch { }
                                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Storage from MachineComponents '{compName}'");
                                                        }
                                                        else if (remoteObj is RemoteLock rlock)
                                                        {
                                                            Lock = rlock;
                                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': set Lock from MachineComponents '{compName}'");
                                                        }
                                                        else if (remoteObj is RemoteComponent component)
                                                        {
                                                            Components[compName] = component;
                                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Component from MachineComponents '{compName}'");
                                                        }
                                                        else if (remoteObj is RemoteSkill skill)
                                                        {
                                                            Methods[compName] = new RemoteMethod(compName, compId, _client);
                                                            Methods[$"skill:{compName}"] = skill;
                                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Skill from MachineComponents '{compName}'");
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    // fallback: treat as generic component
                                                    var comp = new RemoteComponent(compName, compId, _client, _remoteServer);
                                                    lock (dictLock) { Components[compName] = comp; }
                                                    UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Component from MachineComponents (fallback) '{compName}'");
                                                }
                                            }
                                            catch { }
                                        }
                                    }
                                }
                                catch { }
                                return;
                            }

                            // Heuristic: detect Components by checking for common child folders
                            try
                            {
                                var checkBrowser = new Browser(session)
                                {
                                    BrowseDirection = BrowseDirection.Forward,
                                    ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                                    NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                                };
                                var childRefs = await checkBrowser.BrowseAsync(childId);
                                if (childRefs != null && childRefs.Count > 0)
                                {
                                    foreach (var cr in childRefs)
                                    {
                                        var childName = cr?.DisplayName?.Text ?? cr?.BrowseName?.Name ?? string.Empty;
                                        if (string.Equals(childName, "SkillSet", StringComparison.OrdinalIgnoreCase)
                                            || string.Equals(childName, "Monitoring", StringComparison.OrdinalIgnoreCase)
                                            || string.Equals(childName, "MethodSet", StringComparison.OrdinalIgnoreCase)
                                            || string.Equals(childName, "Attributes", StringComparison.OrdinalIgnoreCase)
                                            || string.Equals(childName, "Components", StringComparison.OrdinalIgnoreCase)
                                            || string.Equals(childName, "Resources", StringComparison.OrdinalIgnoreCase))
                                        {
                                            // treat this node as a Component
                                            try
                                            {
                                                var component = new RemoteComponent(name, childId, _client, _remoteServer);
                                                lock (dictLock)
                                                {
                                                    Components[name] = component;
                                                }
                                                UAClient.Common.Log.Debug($"RemoteModule '{Name}': heuristic added Component '{name}'");
                                                return;
                                            }
                                            catch { }
                                        }
                                    }
                                }
                            }
                            catch { }

                            // If this is a SkillSet container, enumerate children and create skills
                            if (string.Equals(browseName, "SkillSet", StringComparison.OrdinalIgnoreCase) || string.Equals(name, "SkillSet", StringComparison.OrdinalIgnoreCase) || (!string.IsNullOrEmpty(childTypeBrowseName) && childTypeBrowseName.IndexOf("SkillSetType", StringComparison.OrdinalIgnoreCase) >= 0))
                             {
                                 try
                                 {
                                     var browser2 = new Browser(session)
                                     {
                                         BrowseDirection = BrowseDirection.Forward,
                                         ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                                         NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
                                     };
                                     var refs2 = await browser2.BrowseAsync(childId);
                                     if (refs2 != null)
                                     {
                                        foreach (var r2 in refs2)
                                        {
                                            try
                                            {
                                                if (r2?.NodeId == null) continue;
                                                var expanded2 = r2.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r2.NodeId);
                                                var childSkillId = UaHelpers.ToNodeId(expanded2, session);
                                                if (childSkillId == null) continue;
                                                var childName = r2.DisplayName?.Text ?? r2.BrowseName?.Name ?? childSkillId.ToString();
                                                // skip type-definition nodes like SkillSetType which are not actual skills
                                                if (string.Equals(childName, "SkillSetType", StringComparison.OrdinalIgnoreCase))
                                                {
                                                    UAClient.Common.Log.Debug($"RemoteModule '{Name}': skipping type node {childName}");
                                                    continue;
                                                }
                                                // Use factory to create skill (module-level skills)
                                                var skillObj = await CreateRemoteObjectFromTypeDefinitionAsync(session, childSkillId, childName);
                                                if (skillObj is RemoteSkill rs)
                                                {
                                                    lock (dictLock)
                                                    {
                                                        SkillSet[childName] = rs;
                                                        // keep compatibility: also register in Methods
                                                        Methods[childName] = new RemoteMethod(childName, childSkillId, _client);
                                                        Methods[$"skill:{childName}"] = rs;
                                                    }
                                                    UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Skill '{childName}' to SkillSet and Methods");
                                                }
                                                else
                                                {
                                                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': failed to create skill for {childName}");
                                                }
                                            }
                                            catch { }
                                        }
                                     }
                                 }
                                 catch { }
                                 return;
                             }

                            // Heuristics based on browse name and display name
                            if (browseName.IndexOf("Skill", StringComparison.OrdinalIgnoreCase) >= 0 || name.IndexOf("Skill", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                UAClient.Common.Log.Debug($"RemoteModule '{Name}': creating RemoteSkill for {name}");
                                lock (dictLock)
                                {
                                    Methods[name] = new RemoteMethod(name, childId, _client); // compatibility
                                    Methods[$"skill:{name}"] = new RemoteSkill(name, childId, _client, _remoteServer);
                                }
                            }
                            else
                            {
                                // Try to create object based on TypeDefinition
                                var remoteObj = await CreateRemoteObjectFromTypeDefinitionAsync(session, childId, name);
                                if (remoteObj != null)
                                {
                                    lock (dictLock)
                                    {
                                        if (remoteObj is RemoteLock lockObj)
                                        {
                                            // set single Lock instance
                                            Lock = lockObj;
                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': set Lock '{name}'");
                                        }
                                        else if (remoteObj is RemotePort port)
                                        {
                                            Ports[name] = port;
                                            // also register the same instance in Components for unified access
                                            try { Components[name] = port; } catch { }
                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Port '{name}'");
                                        }
                                        else if (remoteObj is RemoteStorage storage)
                                        {
                                            Storages[name] = storage;
                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Storage '{name}'");
                                        }
                                        else if (remoteObj is RemoteComponent component)
                                        {
                                            Components[name] = component;
                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Component '{name}'");
                                        }
                                        else if (remoteObj is RemoteSkill skill)
                                        {
                                            Methods[name] = new RemoteMethod(name, childId, _client);
                                            Methods[$"skill:{name}"] = skill;
                                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': added Skill '{name}'");
                                        }
                                    }
                                }
                                else
                                {
                                    // generic method/object — only create a RemoteMethod entry if node is actually a Method
                                    if (r.NodeClass == NodeClass.Method)
                                    {
                                        lock (dictLock) { Methods[name] = new RemoteMethod(name, childId, _client); }
                                    }
                                }
                            }
                        }
                        catch { }
                    }));
                }

                // await processing of this batch of children
                try { await System.Threading.Tasks.Task.WhenAll(tasks); } catch { }
            }
        }

        public async System.Threading.Tasks.Task SetupSubscriptionsAsync(SubscriptionManager? subscriptionManager = null, bool createSubscriptions = true)
        {
            // ensure methods discovered
            await DiscoverMethodsAsync();

            // create a SubscriptionManager if caller didn't provide one
            if (subscriptionManager == null)
            {
                try { subscriptionManager = new SubscriptionManager(_client); }
                catch { /* if creation fails, continue without subscriptions */ }
            }

            // ensure Components are set up
            foreach (var c in Components.Values)
            {
                try { await c.SetupSubscriptionsAsync(subscriptionManager, createSubscriptions); } catch { }
            }

            foreach (var m in Methods.Values)
            {
                try { await m.SetupSubscriptionsAsync(subscriptionManager, true); } catch { }
            }
            // Also subscribe to module variables (Locks, Ports, Storages) similar to pyuaadapter
            try
            {
                // Locks: prefer single Lock; always set client references so reads/writes work
                if (Lock != null)
                {
                    foreach (var lv in Lock.Variables.Values) lv.SetClient(_client);
                    // Only create subscriptions if explicitly requested
                    if (createSubscriptions && subscriptionManager != null)
                    {
                        foreach (var kv in Lock.Variables.Values)
                        {
                            try { await kv.SetupSubscriptionAsync(subscriptionManager); } catch { }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"SetupSubscriptionsAsync: failed to subscribe variables: {ex.Message}");
            }

            // Subscribe to Monitoring variables (only when requested). Always set client reference.
            try
            {
                foreach (var kv in _monitoring_nodes)
                {
                    try
                    {
                        kv.Value.SetClient(_client);
                        if (createSubscriptions && subscriptionManager != null)
                        {
                            try { await kv.Value.SetupSubscriptionAsync(subscriptionManager); } catch { }
                        }
                    }
                    catch { }
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"SetupSubscriptionsAsync: failed to subscribe monitoring variables: {ex.Message}");
            }
        }

        /// <summary>
        /// Attempt to resolve Resource NodeIds for carrier/product type references found in storage slots.
        /// This resolves values like ExpandedNodeId, NodeId strings or dotted browse paths
        /// such as "CA-Module.Resources.Cab_A_Blue.Identification.ComponentName" to a NodeId
        /// using the server-side TranslateBrowsePaths service and caches results via SessionBrowseCache.
        /// </summary>
        public async Task ResolveResourceNodeIdsAsync()
        {
            try
            {
                var session = _client?.Session;
                if (session == null) return;

                foreach (var storKv in Storages)
                {
                    var storage = storKv.Value;
                    if (storage == null) continue;
                    foreach (var slotKv in storage.Slots)
                    {
                        var slot = slotKv.Value;
                        if (slot == null) continue;
                        try
                        {
                            // Helper local: attempt to resolve a string dotted path by trying several start nodes
                            async System.Threading.Tasks.Task<NodeId?> TryResolveDottedPathAsync(string dotted)
                            {
                                if (string.IsNullOrEmpty(dotted)) return null;
                                var parts = dotted.Split('.');
                                // Try a set of start nodes: slot, storage, module base, Objects folder
                                var starts = new NodeId?[] { slot.BaseNodeId, storage.BaseNodeId, BaseNodeId, ObjectIds.ObjectsFolder };
                                foreach (var start in starts)
                                {
                                    try
                                    {
                                        UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: trying TranslatePath from {start} for path '{dotted}'");
                                        var resolved = await SessionBrowseCache.TranslatePathAsync(session, start, parts);
                                        if (resolved != null)
                                        {
                                            UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: TranslatePath succeeded from {start} -> {resolved}");
                                            return resolved;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: TranslatePath from {start} failed: {ex.Message}");
                                    }
                                }
                                return null;
                            }

                            // Resolve CarrierType
                            try
                            {
                                object? cv = null;
                                if (slot.Variables.TryGetValue("CarrierType", out var carrierVar)) cv = carrierVar?.Value;
                                UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: slot='{slot.Name}' CarrierType raw='{cv?.ToString() ?? "(null)"}' (type={cv?.GetType().Name ?? "null"})");

                                if (cv is NodeId nid)
                                {
                                    slot.ResolvedCarrierTypeNodeId = nid;
                                    UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: CarrierType is NodeId -> {nid}");
                                }
                                else if (cv is ExpandedNodeId exp)
                                {
                                    try
                                    {
                                        var to = UaHelpers.ToNodeId(exp, session);
                                        slot.ResolvedCarrierTypeNodeId = to;
                                        UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: CarrierType ExpandedNodeId -> {to}");
                                    }
                                    catch (Exception ex) { UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: ToNodeId(exp) failed: {ex.Message}"); }
                                }
                                else if (cv is string s && !string.IsNullOrEmpty(s))
                                {
                                    NodeId? resolved = null;
                                    try { resolved = new NodeId(s); UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: parsed CarrierType string as NodeId -> {resolved}"); } catch { resolved = null; }
                                    if (resolved == null && s.Contains('.'))
                                    {
                                        resolved = await TryResolveDottedPathAsync(s);
                                    }
                                    if (resolved != null)
                                    {
                                        slot.ResolvedCarrierTypeNodeId = resolved;
                                    }
                                    else
                                    {
                                        UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: CarrierType string '{s}' could not be resolved to NodeId");
                                    }
                                }

                                // If we have a resolved NodeId for the carrier type, attempt to read Identification.ComponentName for a human-friendly name
                                if (slot.ResolvedCarrierTypeNodeId != null && !slot.ResolvedCarrierTypeNodeId.Equals(NodeId.Null))
                                {
                                    try
                                    {
                                        // Always use DisplayName as requested
                                        var readDisplay = new ReadValueIdCollection { new ReadValueId { NodeId = slot.ResolvedCarrierTypeNodeId, AttributeId = Attributes.DisplayName } };
                                        DataValueCollection resultsDisp;
                                        DiagnosticInfoCollection diagDisp;
                                        session.Read(null, 0, TimestampsToReturn.Neither, readDisplay, out resultsDisp, out diagDisp);
                                        if (resultsDisp != null && resultsDisp.Count > 0 && resultsDisp[0].Value != null)
                                        {
                                            var dn = resultsDisp[0].Value as LocalizedText;
                                            slot.ResolvedCarrierTypeName = dn?.Text ?? dn?.ToString() ?? slot.ResolvedCarrierTypeNodeId.ToString();
                                            UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: used DisplayName='{slot.ResolvedCarrierTypeName}' for {slot.ResolvedCarrierTypeNodeId}");
                                        }
                                        else
                                        {
                                            // Fallback to NodeId string
                                            slot.ResolvedCarrierTypeName = slot.ResolvedCarrierTypeNodeId.ToString();
                                            UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: DisplayName empty, using NodeId='{slot.ResolvedCarrierTypeName}'");
                                        }
                                    }
                                    catch (Exception ex) { UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: read DisplayName failed: {ex.Message}"); slot.ResolvedCarrierTypeName = slot.ResolvedCarrierTypeNodeId.ToString(); }
                                }
                            }
                            catch (Exception ex) { UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: CarrierType resolve error: {ex.Message}"); }

                            // Resolve ProductType (same strategy)
                            try
                            {
                                object? pv = null;
                                if (slot.Variables.TryGetValue("ProductType", out var productVar)) pv = productVar?.Value;
                                UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: slot='{slot.Name}' ProductType raw='{pv?.ToString() ?? "(null)"}' (type={pv?.GetType().Name ?? "null"})");

                                if (pv is NodeId pnid)
                                {
                                    slot.ResolvedProductTypeNodeId = pnid;
                                    UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: ProductType is NodeId -> {pnid}");
                                }
                                else if (pv is ExpandedNodeId pexp)
                                {
                                    try
                                    {
                                        var to2 = UaHelpers.ToNodeId(pexp, session);
                                        slot.ResolvedProductTypeNodeId = to2;
                                        UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: ProductType ExpandedNodeId -> {to2}");
                                    }
                                    catch (Exception ex) { UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: ToNodeId(Expanded) failed: {ex.Message}"); }
                                }
                                else if (pv is string ps && !string.IsNullOrEmpty(ps))
                                {
                                    NodeId? presolved = null;
                                    try { presolved = new NodeId(ps); UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: parsed ProductType string as NodeId -> {presolved}"); } catch { presolved = null; }
                                    if (presolved == null && ps.Contains('.'))
                                    {
                                        presolved = await TryResolveDottedPathAsync(ps);
                                    }
                                    if (presolved != null)
                                    {
                                        slot.ResolvedProductTypeNodeId = presolved;
                                    }
                                    else
                                    {
                                        UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: ProductType string '{ps}' could not be resolved to NodeId");
                                    }
                                }

                                if (slot.ResolvedProductTypeNodeId != null && !slot.ResolvedProductTypeNodeId.Equals(NodeId.Null))
                                {
                                    try
                                    {
                                        // Always use DisplayName as requested
                                        var readDisplay2 = new ReadValueIdCollection { new ReadValueId { NodeId = slot.ResolvedProductTypeNodeId, AttributeId = Attributes.DisplayName } };
                                        DataValueCollection resultsDisp2;
                                        DiagnosticInfoCollection diagDisp2;
                                        session.Read(null, 0, TimestampsToReturn.Neither, readDisplay2, out resultsDisp2, out diagDisp2);
                                        if (resultsDisp2 != null && resultsDisp2.Count > 0 && resultsDisp2[0].Value != null)
                                        {
                                            var dn2 = resultsDisp2[0].Value as LocalizedText;
                                            slot.ResolvedProductTypeName = dn2?.Text ?? dn2?.ToString() ?? slot.ResolvedProductTypeNodeId.ToString();
                                            UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: used DisplayName='{slot.ResolvedProductTypeName}' for {slot.ResolvedProductTypeNodeId}");
                                        }
                                        else
                                        {
                                            // Fallback to NodeId string
                                            slot.ResolvedProductTypeName = slot.ResolvedProductTypeNodeId.ToString();
                                            UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: DisplayName empty, using NodeId='{slot.ResolvedProductTypeName}'");
                                        }
                                    }
                                    catch (Exception ex) { UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: read DisplayName failed: {ex.Message}"); slot.ResolvedProductTypeName = slot.ResolvedProductTypeNodeId.ToString(); }
                                }
                            }
                            catch (Exception ex) { UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: ProductType resolve error: {ex.Message}"); }
                        }
                        catch (Exception ex)
                        {
                            UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: slot '{slot?.Name}' handling failed: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Debug($"ResolveResourceNodeIdsAsync: failed: {ex.Message}");
            }
        }

        // Subscribe only core variables for the module: Monitoring variables and Lock variables, and propagate to skills/components
        public async Task SubscribeCoreAsync(SubscriptionManager subscriptionManager)
        {
            if (subscriptionManager == null) return;
            var session = _client.Session ?? throw new InvalidOperationException("No session");
            try
            {
                // Ensure monitoring variables have client and subscribe
                foreach (var kv in _monitoring_nodes)
                {
                    try
                    {
                        kv.Value.SetClient(_client);
                        await kv.Value.SetupSubscriptionAsync(subscriptionManager);
                    }
                    catch { }
                }

                // Lock variables
                if (Lock != null)
                {
                    foreach (var lv in Lock.Variables.Values)
                    {
                        try { lv.SetClient(_client); await lv.SetupSubscriptionAsync(subscriptionManager); } catch { }
                    }
                }

                // Subscribe core for skills and components contained in this module
                foreach (var m in Methods.Values.OfType<RemoteSkill>())
                {
                    try { await m.SubscribeCoreAsync(subscriptionManager); } catch { }
                }
                foreach (var c in Components.Values)
                {
                    try { await c.SubscribeCoreAsync(subscriptionManager); } catch { }
                }
            }
            catch { }
        }

        /// <summary>
        /// Ensure the module's first port is coupled. This checks the couple skill state and starts it if needed.
        /// </summary>
        public async System.Threading.Tasks.Task CoupleAsync(TimeSpan? timeout = null)
        {
            timeout ??= TimeSpan.FromSeconds(30);
            try
            {
                if (Ports.Count == 0) return;
                var firstPort = Ports.Values.First();
                var isCoupled = await firstPort.IsCoupledAsync(_client);
                if (isCoupled)
                {
                    UAClient.Common.Log.Debug($"Module '{Name}': first port '{firstPort.Name}' already coupled");
                    return;
                }
                UAClient.Common.Log.Info($"Module '{Name}': coupling first port '{firstPort.Name}'");
                await firstPort.CoupleAsync(_client, timeout);
                UAClient.Common.Log.Info($"Module '{Name}': coupling attempt finished for port '{firstPort.Name}'");
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"Module '{Name}': CoupleAsync failed: {ex.Message}");
            }
        }

        public async System.Threading.Tasks.Task<bool?> LockAsync(Session session)
        {
            if (session == null) return null;

            // Prefer an already discovered Lock instance
            RemoteLock? lockObj = Lock;

            // If not present, try to discover a candidate under the module node
            if (lockObj == null) return null;

            var ourAppName = _client?.Configuration?.ApplicationName ?? string.Empty;
            var ourHostname = System.Net.Dns.GetHostName();

            string NormalizeOwner(string? ownerValue)
            {
                return string.IsNullOrWhiteSpace(ownerValue)
                    ? string.Empty
                    : ownerValue.Trim('\'', '"', ' ');
            }

            bool OwnedByUs(string ownerValue)
            {
                if (string.IsNullOrEmpty(ownerValue)) return false;

                return (!string.IsNullOrEmpty(ourAppName) && ownerValue.IndexOf(ourAppName, StringComparison.OrdinalIgnoreCase) >= 0) ||
                       (!string.IsNullOrEmpty(ourHostname) && ownerValue.IndexOf(ourHostname, StringComparison.OrdinalIgnoreCase) >= 0);
            }

            UAClient.Common.Log.Info($"Locking module {Name} via lock {lockObj.Name}");
            try
            {
                UAClient.Common.Log.Debug($"RemoteModule '{Name}': invoking lock Init on {lockObj.Name} (node={lockObj.BaseNodeId})");
                var initOk = await lockObj.InitLockAsync(session);
                if (!initOk)
                {
                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': InitLock call did not succeed (or no lock method found)");
                    IsLockedByUs = false;
                    return false;
                }

                var waitTimeout = TimeSpan.FromSeconds(10);
                var lockedNow = await lockObj.WaitForLockedAsync(session, waitTimeout);

                if (!lockedNow)
                {
                    UAClient.Common.Log.Warn($"Module lock not confirmed for {Name} after {waitTimeout.TotalSeconds}s");
                    IsLockedByUs = false;
                    return false;
                }

                try
                {
                    var owner = NormalizeOwner(await lockObj.GetLockOwnerAsync(session));

                    UAClient.Common.Log.Debug($"RemoteModule '{Name}': Lock owner='{owner}', AppName='{ourAppName}', Hostname='{ourHostname}'");

                    if (!string.IsNullOrEmpty(owner))
                    {
                        if (OwnedByUs(owner))
                        {
                            UAClient.Common.Log.Info($"RemoteModule '{Name}': Successfully locked by us (Owner: {owner})");
                            IsLockedByUs = true;
                            return true;
                        }

                        UAClient.Common.Log.Warn($"RemoteModule '{Name}': Module is locked by different client. Owner: '{owner}', Our AppName: '{ourAppName}', Our Hostname: '{ourHostname}'");
                        IsLockedByUs = false;
                        return false;
                    }

                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': Could not determine lock owner, assuming lock is ours");
                    IsLockedByUs = true;
                    return true;
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': Owner verification failed: {ex.Message}. Assuming lock is ours.");
                    IsLockedByUs = true;
                    return true;
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"Lock init failed: {ex.Message}");
                IsLockedByUs = false;
                return null;
            }
        }

        public async System.Threading.Tasks.Task<bool?> UnlockAsync(Session session)
        {
            if (session == null) return null;

            var lockObj = Lock;
            if (lockObj == null) return null;
    
            UAClient.Common.Log.Info($"Unlocking module {Name} via lock {lockObj.Name}");
            try
            {
                await lockObj.ReleaseLockAsync(session);
                var locked = await lockObj.IsLockedAsync(session);
                var lockedText = locked.HasValue ? (locked.Value ? "true" : "false") : "unknown";
                UAClient.Common.Log.Info($"Module locked state after unlock={lockedText}");
                
                IsLockedByUs = false;  // NEW: Reset flag
                
                return locked;
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"Unlock failed: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Start the module's startup skill. Optionally perform coupling first (default=true).
        /// </summary>
        public async System.Threading.Tasks.Task StartAsync(bool reset = true, TimeSpan? timeout = null, bool couple = true)
        {
            timeout ??= TimeSpan.FromSeconds(10);
            // perform coupling first if requested
            if (couple)
            {
                await CoupleAsync(timeout);
            }
            
            var session = _client?.Session ?? throw new InvalidOperationException("No session");

            var startupSkill = Methods.Values
                .OfType<RemoteSkill>()
                .FirstOrDefault(s => s.Name.IndexOf("Startup", StringComparison.OrdinalIgnoreCase) >= 0);
            if (startupSkill == null)
            {
                UAClient.Common.Log.Info($"No startup skill found in module {Name}");
                return;
            }

            // use global subscription manager from RemoteServer (injected during construction or available via _remoteServer property)
            var subMgr = _remoteServer?.SubscriptionManager;
            if (subMgr != null)
            {
                try { await startupSkill.SetupSubscriptionsAsync(subMgr, true); } catch { }
            }

            // get current state and do reset logging
            var st = await startupSkill.GetStateAsync();
            if (reset && (st == (int)UAClient.Common.SkillStates.Halted || st == (int)UAClient.Common.SkillStates.Completed))
            {
                UAClient.Common.Log.Info($"Resetting startup skill {startupSkill.Name} before start (state={st})");
            }

            if (st == null || st != (int)UAClient.Common.SkillStates.Running)
            {
                if (startupSkill.IsFinite)
                {
                    await ((dynamic)startupSkill).ExecuteAsync(null, waitForCompletion: true, resetAfterCompletion: true, resetBeforeIfHalted: reset, timeout: TimeSpan.FromSeconds(60));
                    UAClient.Common.Log.Info($"Startup finite skill executed");
                }
                else
                {
                    await ((dynamic)startupSkill).ExecuteAsync(null, waitForCompletion: false, resetBeforeIfHalted: reset, timeout: TimeSpan.FromSeconds(30));
                    var ok = await ((dynamic)startupSkill).WaitForStateAsync(UAClient.Common.SkillStates.Running, TimeSpan.FromSeconds(30));
                    UAClient.Common.Log.Info($"Startup skill reached Running={ok}");
                }
            }
            else
            {
                UAClient.Common.Log.Warn($"Startup skill already running");
            }
        }

        // Evaluate readiness by checking lock state, first port coupling and startup skill state
        public async Task EvaluateReadyAsync()
        {
            try
            {
                var session = _client?.Session;
                if (session == null) { IsReady = false; return; }

                // Check locked
                var locked = false;
                try
                {
                    if (Lock != null)
                    {
                        var l = await Lock.IsLockedAsync(session);
                        locked = l.HasValue && l.Value;
                    }
                }
                catch { locked = false; }

                // Check coupled (first port)
                var coupled = false;
                try
                {
                    if (Ports.Count > 0)
                    {
                        var firstPort = Ports.Values.First();
                        coupled = await firstPort.IsCoupledAsync(_client);
                    }
                    else
                    {
                        coupled = true; // no ports means coupling not required
                    }
                }
                catch { coupled = false; }

                // Check startup skill running
                var startupRunning = false;
                try
                {
                    var startupSkill = Methods.Values.OfType<RemoteSkill>().FirstOrDefault(s => s.Name.IndexOf("Startup", StringComparison.OrdinalIgnoreCase) >= 0);
                    if (startupSkill == null) startupRunning = true; // no startup skill => consider satisfied
                    else
                    {
                        var st = await startupSkill.GetStateAsync();
                        startupRunning = (st != null && st == (int)UAClient.Common.SkillStates.Running);
                    }
                }
                catch { startupRunning = false; }

                IsReady = locked && coupled && startupRunning;
            }
            catch { IsReady = false; }
        }

        // Make the module ready: lock, couple first port and start startup skill. Non-failing if already satisfied.
        public async Task<bool> MakeReadyAsync(TimeSpan? timeout = null)
        {
            timeout ??= TimeSpan.FromSeconds(60);
            var session = _client?.Session ?? throw new InvalidOperationException("No session");

            // ensure we have subscriptions for core so we can react to spontaneous changes
            try { await SubscribeCoreAsync(_remoteServer?.SubscriptionManager ?? new SubscriptionManager(_client)); } catch { }

            // 1) lock module
            try
            {
                var lockedNow = await LockAsync(session);
                // continue even if null
            }
            catch { }

            // 2) couple first port
            try
            {
                await CoupleAsync(timeout);
            }
            catch { }

            // 3) start startup skill
            try
            {
                await StartAsync(reset: true, timeout: TimeSpan.FromSeconds(30), couple: false);
            }
            catch { }

            // evaluate readiness and return
            await EvaluateReadyAsync();
            return IsReady;
        }

        private async System.Threading.Tasks.Task<bool> IsTypeOrSubtypeOfAsync(Session session, NodeId nodeId, System.Collections.Generic.IReadOnlySet<string> baseTypeNames)
        {
            var currentType = await GetTypeDefinitionAsync(session, nodeId);
            while (currentType != null)
            {
                var browseName = await GetBrowseNameAsync(session, currentType);
                if (browseName != null && baseTypeNames.Contains(browseName))
                {
                    return true;
                }
                // Get the supertype
                try
                {
                    var browser = new Browser(session)
                    {
                        BrowseDirection = BrowseDirection.Inverse,
                        ReferenceTypeId = ReferenceTypeIds.HasSubtype,
                        IncludeSubtypes = false,
                        NodeClassMask = (int)NodeClass.ObjectType
                    };
                    var refs = await browser.BrowseAsync(currentType);
                    if (refs != null && refs.Count > 0)
                    {
                        var expanded = refs[0].NodeId as ExpandedNodeId ?? new ExpandedNodeId(refs[0].NodeId);
                        currentType = UaHelpers.ToNodeId(expanded, session);
                    }
                    else
                    {
                        currentType = null;
                    }
                }
                catch
                {
                    currentType = null;
                }
            }
            return false;
        }

        // Auto-ready control
        private bool _autoReadyEnabled = false;
        private CancellationTokenSource? _autoReadyCts;
        private Task? _autoReadyTask;
        private readonly object _autoReadyLock = new object();
        private readonly List<MonitoredItem> _autoReadyMonitoredItems = new List<MonitoredItem>();
        private int _makeReadyInProgress = 0; // atomic flag

        // Auto-recovery control (NEW - subscription-based recovery)
        private bool _autoRecoveryEnabled = false;
        private readonly List<MonitoredItem> _autoRecoveryMonitoredItems = new List<MonitoredItem>();
        private int _recoveryInProgress = 0; // atomic flag

        /// <summary>
        /// Enable auto-recovery: subscribe to Lock state and StartupSkill state,
        /// automatically trigger recovery (halt all skills, re-lock, restart startup) on loss.
        /// </summary>
        public async Task EnableAutoRecoveryAsync()
        {
            if (_autoRecoveryEnabled) return;
            _autoRecoveryEnabled = true;

            try
            {
                var subMgr = _remoteServer?.SubscriptionManager;
                var session = _client?.Session;
                if (subMgr == null || session == null)
                {
                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': Cannot enable auto-recovery - no SubscriptionManager or Session");
                    return;
                }

                // 1. Subscribe to Lock state
                if (Lock != null)
                {
                    try
                    {
                        if (Lock.CurrentState != null)
                        {
                            UAClient.Common.Log.Info($"RemoteModule '{Name}': Subscribing to Lock.CurrentState for auto-recovery");
                            var mi = await subMgr.AddMonitoredItemAsync(Lock.CurrentState.NodeId, 
                                (m, e) => { _ = Task.Run(async () => await OnRecoveryLockChangedAsync()); });
                            lock (_autoRecoveryMonitoredItems) { _autoRecoveryMonitoredItems.Add(mi); }
                        }
                        else if (Lock.Variables.TryGetValue("Locked", out var lockedVar))
                        {
                            UAClient.Common.Log.Info($"RemoteModule '{Name}': Subscribing to Lock.Locked for auto-recovery");
                            var mi = await subMgr.AddMonitoredItemAsync(lockedVar.NodeId, 
                                (m, e) => { _ = Task.Run(async () => await OnRecoveryLockChangedAsync()); });
                            lock (_autoRecoveryMonitoredItems) { _autoRecoveryMonitoredItems.Add(mi); }
                        }

                        // Also subscribe to LockingClient to detect ownership changes
                        if (Lock.LockingClient != null)
                        {
                            UAClient.Common.Log.Info($"RemoteModule '{Name}': Subscribing to Lock.LockingClient for auto-recovery");
                            var mi2 = await subMgr.AddMonitoredItemAsync(Lock.LockingClient.NodeId, 
                                (m, e) => { _ = Task.Run(async () => await OnRecoveryLockChangedAsync()); });
                            lock (_autoRecoveryMonitoredItems) { _autoRecoveryMonitoredItems.Add(mi2); }
                        }
                    }
                    catch (Exception ex)
                    {
                        UAClient.Common.Log.Warn($"RemoteModule '{Name}': Failed to subscribe to Lock state: {ex.Message}");
                    }
                }

                // 2. Subscribe to StartupSkill state
                try
                {
                    var startupSkill = Methods.Values
                        .OfType<RemoteSkill>()
                        .FirstOrDefault(s => s.Name.IndexOf("Startup", StringComparison.OrdinalIgnoreCase) >= 0);

                    if (startupSkill != null)
                    {
                        // Ensure StartupSkill has subscriptions set up
                        try { await startupSkill.SetupSubscriptionsAsync(subMgr, true); } catch { }
                        
                        if (startupSkill.CurrentStateNode != null)
                        {
                            UAClient.Common.Log.Info($"RemoteModule '{Name}': Subscribing to StartupSkill.CurrentState for auto-recovery");
                            var mi = await subMgr.AddMonitoredItemAsync(startupSkill.CurrentStateNode, 
                                (m, e) => { _ = Task.Run(async () => await OnRecoveryStartupChangedAsync()); });
                            lock (_autoRecoveryMonitoredItems) { _autoRecoveryMonitoredItems.Add(mi); }
                        }
                        else
                        {
                            UAClient.Common.Log.Warn($"RemoteModule '{Name}': StartupSkill.CurrentStateNode is null, cannot subscribe for auto-recovery");
                        }
                    }
                    else
                    {
                        UAClient.Common.Log.Info($"RemoteModule '{Name}': No StartupSkill found, skipping startup state subscription");
                    }
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': Failed to subscribe to StartupSkill state: {ex.Message}");
                }

                UAClient.Common.Log.Info($"RemoteModule '{Name}': Auto-recovery enabled with {_autoRecoveryMonitoredItems.Count} subscriptions");
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Error($"RemoteModule '{Name}': Failed to enable auto-recovery: {ex.Message}");
            }
        }

        public async Task DisableAutoRecoveryAsync()
        {
            _autoRecoveryEnabled = false;
            try
            {
                var subMgr = _remoteServer?.SubscriptionManager;
                if (subMgr != null)
                {
                    lock (_autoRecoveryMonitoredItems)
                    {
                        foreach (var mi in _autoRecoveryMonitoredItems)
                        {
                            try { _ = subMgr.RemoveMonitoredItemAsync(mi); } catch { }
                        }
                        _autoRecoveryMonitoredItems.Clear();
                    }
                }
                UAClient.Common.Log.Info($"RemoteModule '{Name}': Auto-recovery disabled");
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Debug($"RemoteModule '{Name}': DisableAutoRecoveryAsync failed: {ex.Message}");
            }
        }

        private async Task OnRecoveryLockChangedAsync()
        {
            try
            {
                if (!_autoRecoveryEnabled) return;

                var session = _client?.Session;
                if (session == null || Lock == null) return;

                UAClient.Common.Log.Debug($"RemoteModule '{Name}': OnRecoveryLockChanged - checking lock state");

                var locked = await Lock.IsLockedAsync(session);
                UAClient.Common.Log.Debug($"RemoteModule '{Name}': Lock state = {locked}");

                // If lock is lost, trigger recovery
                if (locked.HasValue && !locked.Value)
                {
                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': 🔓 Lock lost detected! Triggering recovery...");
                    await TriggerRecoveryAsync("Lock lost");
                }
                else if (locked.HasValue && locked.Value)
                {
                    // Lock is present - check if WE are the owner
                    try
                    {
                        var owner = await Lock.GetLockOwnerAsync(session);
                        var ourId = _client?.Configuration?.ApplicationName ?? string.Empty;
                        var ourHostname = System.Net.Dns.GetHostName();

                        if (!string.IsNullOrEmpty(owner))
                        {
                            owner = owner.Trim('\'', '"', ' ');
                            
                            bool isOurs = (!string.IsNullOrEmpty(ourId) && owner.IndexOf(ourId, StringComparison.OrdinalIgnoreCase) >= 0) ||
                                         (!string.IsNullOrEmpty(ourHostname) && owner.IndexOf(ourHostname, StringComparison.OrdinalIgnoreCase) >= 0);

                            if (!isOurs)
                            {
                                UAClient.Common.Log.Warn($"RemoteModule '{Name}': 🔒 Lock stolen by '{owner}'! Triggering recovery...");
                                await TriggerRecoveryAsync($"Lock stolen by {owner}");
                            }
                            else
                            {
                                UAClient.Common.Log.Debug($"RemoteModule '{Name}': Lock is ours (owner={owner})");
                                IsLockedByUs = true;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        UAClient.Common.Log.Debug($"RemoteModule '{Name}': Failed to check lock owner: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Error($"RemoteModule '{Name}': OnRecoveryLockChanged error: {ex.Message}");
            }
        }

        private async Task OnRecoveryStartupChangedAsync()
        {
            try
            {
                if (!_autoRecoveryEnabled) return;

                var startupSkill = Methods.Values
                    .OfType<RemoteSkill>()
                    .FirstOrDefault(s => s.Name.IndexOf("Startup", StringComparison.OrdinalIgnoreCase) >= 0);

                if (startupSkill == null) return;

                UAClient.Common.Log.Debug($"RemoteModule '{Name}': OnRecoveryStartupChanged - checking startup state");

                var state = await startupSkill.GetStateAsync();
                UAClient.Common.Log.Debug($"RemoteModule '{Name}': StartupSkill state = {state}");

                // If StartupSkill is Halted (should be Running), trigger recovery
                if (state.HasValue && state.Value == (int)UAClient.Common.SkillStates.Halted)
                {
                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': ⛔ StartupSkill halted! Triggering recovery...");
                    await TriggerRecoveryAsync("StartupSkill halted");
                }
                else if (state.HasValue && state.Value == (int)UAClient.Common.SkillStates.Running)
                {
                    UAClient.Common.Log.Debug($"RemoteModule '{Name}': StartupSkill is running");
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Error($"RemoteModule '{Name}': OnRecoveryStartupChanged error: {ex.Message}");
            }
        }

        private async Task TriggerRecoveryAsync(string reason)
        {
            // Use atomic flag to prevent multiple concurrent recoveries
            if (System.Threading.Interlocked.CompareExchange(ref _recoveryInProgress, 1, 0) != 0)
            {
                UAClient.Common.Log.Debug($"RemoteModule '{Name}': Recovery already in progress, skipping");
                return;
            }

            try
            {
                UAClient.Common.Log.Warn($"RemoteModule '{Name}': 🔄 Starting recovery - Reason: {reason}");

                var session = _client?.Session;
                if (session == null)
                {
                    UAClient.Common.Log.Error($"RemoteModule '{Name}': Cannot recover - no session");
                    return;
                }

                // Step 1: Re-lock module FIRST (MUST have lock before manipulating skills!)
                UAClient.Common.Log.Info($"RemoteModule '{Name}': Recovery Step 1/3 - Re-locking module...");
                try
                {
                    var lockResult = await LockAsync(session);
                    if (lockResult == true)
                    {
                        UAClient.Common.Log.Info($"RemoteModule '{Name}': ✓ Module re-locked");
                    }
                    else
                    {
                        UAClient.Common.Log.Error($"RemoteModule '{Name}': ❌ Failed to re-lock module, aborting recovery");
                        return;
                    }
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Error($"RemoteModule '{Name}': Error re-locking: {ex.Message}");
                    return;
                }

                // Step 2: NOW we can halt all skills (we have the lock!)
                UAClient.Common.Log.Info($"RemoteModule '{Name}': Recovery Step 2/3 - Halting all skills...");
                try
                {
                    var skills = Methods.Values.OfType<RemoteSkill>().ToList();
                    var haltTasks = new List<Task>();

                    foreach (var skill in skills)
                    {
                        if (skill.CurrentState != UAClient.Common.SkillStates.Halted)
                        {
                            UAClient.Common.Log.Debug($"RemoteModule '{Name}': Halting skill '{skill.Name}'");
                            haltTasks.Add(Task.Run(async () =>
                            {
                                try { await skill.HaltAsync(); }
                                catch (Exception ex) { UAClient.Common.Log.Debug($"Halt '{skill.Name}' failed: {ex.Message}"); }
                            }));
                        }
                    }

                    if (haltTasks.Count > 0)
                    {
                        await Task.WhenAll(haltTasks);
                        // Wait for skills to reach Halted state
                        await Task.Delay(2000);
                    }

                    UAClient.Common.Log.Info($"RemoteModule '{Name}': ✓ All skills halted");
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteModule '{Name}': Error halting skills: {ex.Message}");
                }

                // Step 3: Restart StartupSkill
                UAClient.Common.Log.Info($"RemoteModule '{Name}': Recovery Step 3/3 - Restarting StartupSkill...");
                try
                {
                    await StartAsync(reset: true, timeout: TimeSpan.FromSeconds(30), couple: false);
                    UAClient.Common.Log.Info($"RemoteModule '{Name}': ✓ StartupSkill restarted");
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Error($"RemoteModule '{Name}': Error restarting startup: {ex.Message}");
                    return;
                }

                UAClient.Common.Log.Warn($"RemoteModule '{Name}': ✅ Recovery completed successfully!");
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Error($"RemoteModule '{Name}': Recovery failed: {ex.Message}");
            }
            finally
            {
                System.Threading.Interlocked.Exchange(ref _recoveryInProgress, 0);
            }
        }
    }
}
