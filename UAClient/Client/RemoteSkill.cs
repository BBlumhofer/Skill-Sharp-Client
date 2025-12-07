using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;

namespace UAClient.Client
{
    public class RemoteSkill : BaseRemoteCallable
    {
        private readonly RemoteServer _remoteServer;
        private List<string>? _availableStates;
        private NodeId? _stateMachineNode;
        private NodeId? _currentStateNode;
        private NodeId? _startMethodNode;
        private NodeId? _resetMethodNode;
        private NodeId? _haltMethodNode;
        private bool? _isFiniteHint = null;

        // cached state + subscribers (mirror Python behaviour)
        private Common.SkillStates _cachedState = Common.SkillStates.Halted;
        private List<Action<Common.SkillStates>> _stateSubscribers = new List<Action<Common.SkillStates>>();
        private readonly object _coreSubLock = new object();
        private bool _coreSubscribed = false;

        public NodeId? StateMachineNode => _stateMachineNode;
        public NodeId? CurrentStateNode => _currentStateNode;
        public NodeId? StartMethodNode => _startMethodNode;
        public NodeId? ResetMethodNode => _resetMethodNode;
        public NodeId? HaltMethodNode => _haltMethodNode;

        // Expose a RemoteVariable for the current state if available via Monitoring map
        public RemoteVariable? CurrentStateVariable
        {
            get
            {
                if (Monitoring.TryGetValue("CurrentState", out var rv)) return rv;
                return null;
            }
        }

        // Expose current state as a property that returns the cached state value (sync access)
        public Common.SkillStates CurrentState => _cachedState;

        public RemoteSkill(string name, NodeId baseNodeId, UaClient client, RemoteServer remoteServer, bool? isFiniteHint = null) : base(name, baseNodeId, client)
        {
            _remoteServer = remoteServer;
            _isFiniteHint = isFiniteHint;
        }

        public async Task<NodeId?> FindMethodNodeRecursive(Session session, NodeId startNode, string methodName)
        {
            // Try fast path: translate common paths to NodeId via server-side TranslateBrowsePaths
            try
            {
                var try1 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { methodName });
                if (try1 != null) return try1;
                var try2 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { "StateMachine", methodName });
                if (try2 != null) return try2;
                var try3 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { "SkillExecution", "StateMachine", methodName });
                if (try3 != null) return try3;
            }
            catch { }

            var browser = new Browser(session)
            {
                BrowseDirection = BrowseDirection.Forward,
                // restrict browsing to HasComponent relationships to avoid wide hierarchical walks
                ReferenceTypeId = ReferenceTypeIds.HasComponent,
                NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
            };
            var queue = new Queue<NodeId>();
            var visited = new HashSet<string>(); // use string form to compare NodeIds safely
            queue.Enqueue(startNode);
            visited.Add(startNode.ToString());
            int inspected = 0;
            const int MaxInspected = 1000; // defensive cutoff to avoid pathological long searches

            while (queue.Count > 0)
            {
                var node = queue.Dequeue();
                if (++inspected > MaxInspected) break;
                ReferenceDescriptionCollection refs = null;
                try
                {
                    refs = await browser.BrowseAsync(node);
                }
                catch { }

                if (refs == null) continue;

                foreach (var r in refs)
                {
                    try
                    {
                        if (r?.NodeId == null) continue;
                        // Check for method-like nodes
                        if (r.NodeClass == NodeClass.Method)
                        {
                            var name = r.DisplayName?.Text ?? r.BrowseName?.Name;
                            if (!string.IsNullOrEmpty(name) && string.Equals(name, methodName, StringComparison.OrdinalIgnoreCase))
                            {
                                var expanded = r.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r.NodeId);
                                return UaHelpers.ToNodeId(expanded, session);
                            }
                        }

                        // enqueue objects/variables to look deeper
                        if (r.NodeClass == NodeClass.Object || r.NodeClass == NodeClass.Variable)
                        {
                            var expanded = r.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r.NodeId);
                            var childId = UaHelpers.ToNodeId(expanded, session);
                            if (childId != null)
                            {
                                var key = childId.ToString();
                                if (!visited.Contains(key))
                                {
                                    visited.Add(key);
                                    queue.Enqueue(childId);
                                }
                            }
                        }
                    }
                    catch { }
                }
            }

            return null;
        }

        public async Task WriteParameterAsync(string parameterName, object value)
        {
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            // prefer ParameterSet map first if available
            if (ParameterSet.TryGetValue(parameterName, out var rv) && rv?.NodeId != null)
            {
                try
                {
                    await RemoteServerClient.WriteNodeAsync(rv.NodeId.ToString(), value);
                    return;
                }
                catch (ServiceResultException sre) when (sre.StatusCode == StatusCodes.BadOutOfRange)
                {
                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': failed to write parameter {parameterName}: BadOutOfRange (via ParameterSet)");
                    throw;
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': failed to write parameter {parameterName}: {ex.Message}");
                    throw;
                }
            }

            // fallback: attempt to locate parameter node dynamically
            NodeId? paramRoot = await FindNodeByBrowseNameRecursive(session, BaseNodeId, "ParameterSet");
            var varNode = paramRoot != null ? await FindVariableNodeRecursive(session, paramRoot, parameterName)
                                           : await FindVariableNodeRecursive(session, BaseNodeId, parameterName);
            if (varNode == null)
            {
                UAClient.Common.Log.Warn($"RemoteSkill '{Name}': parameter '{parameterName}' not found under {BaseNodeId}");
                return;
            }
            UAClient.Common.Log.Debug($"RemoteSkill '{Name}': writing parameter {parameterName} -> {value}");
            // Use UaClient helper if available
            try
            {
                await RemoteServerClient.WriteNodeAsync(varNode.ToString(), value);
            }
            catch (ServiceResultException sre) when (sre.StatusCode == StatusCodes.BadOutOfRange)
            {
                // Attempt to read EURange or other metadata to help diagnosis
                try
                {
                    var eurangeNode = await FindNodeByBrowseNameRecursive(session, varNode, "EURange");
                    object? eurangeVal = null;
                    if (eurangeNode != null)
                    {
                        var dv = await session.ReadValueAsync(eurangeNode, System.Threading.CancellationToken.None);
                        eurangeVal = dv.Value;
                    }
                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': failed to write parameter {parameterName}: BadOutOfRange; attempted value={value}; EURange={eurangeVal}");
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': failed to write parameter {parameterName}: BadOutOfRange (and reading EURange failed): {ex.Message}");
                }
                throw;
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"RemoteSkill '{Name}': failed to write parameter {parameterName}: {ex.Message}");
            }
        }

        private async Task<NodeId?> FindVariableNodeRecursive(Session session, NodeId startNode, string variableName)
        {
            // Try translate-based fast paths
            try
            {
                var t1 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { variableName }); if (t1 != null) return t1;
                var t2 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { "ParameterSet", variableName }); if (t2 != null) return t2;
                var t3 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { "Monitoring", variableName }); if (t3 != null) return t3;
                var t4 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { "SkillExecution", "StateMachine", variableName }); if (t4 != null) return t4;
            }
            catch { }

            var browser = new Browser(session)
            {
                BrowseDirection = BrowseDirection.Forward,
                ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                // include Objects as well so we can traverse through Object nodes to find nested Variables
                NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable)
            };
            var queue = new Queue<NodeId>(); queue.Enqueue(startNode);
            while (queue.Count > 0)
            {
                var node = queue.Dequeue();
                ReferenceDescriptionCollection refs = null;
                try { refs = await browser.BrowseAsync(node); } catch { }
                if (refs == null) continue;
                foreach (var r in refs)
                {
                    try
                    {
                        if (r?.NodeId == null) continue;
                        if (r.NodeClass == NodeClass.Variable)
                        {
                            var name = r.DisplayName?.Text ?? r.BrowseName?.Name;
                            if (!string.IsNullOrEmpty(name) && string.Equals(name, variableName, StringComparison.OrdinalIgnoreCase))
                            {
                                var expanded = r.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r.NodeId);
                                return UaHelpers.ToNodeId(expanded, session);
                            }
                        }
                        if (r.NodeClass == NodeClass.Object || r.NodeClass == NodeClass.Variable)
                        {
                            var expanded = r.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r.NodeId);
                            var childId = UaHelpers.ToNodeId(expanded, session);
                            if (childId != null) queue.Enqueue(childId);
                        }
                    }
                    catch { }
                }
            }
            return null;
        }

        public async Task<int?> GetStateAsync()
        {
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            // try Monitoring CurrentState variable first
            try
            {
                var csv = CurrentStateVariable;
                if (csv != null && csv.Value != null)
                {
                    var v = csv.Value;
                    if (v is int vi) return vi;
                    if (v is uint vui) return (int)vui;
                    if (v is short s) return (int)s;
                    if (v is ushort us) return (int)us;
                    var name = v is LocalizedText lt ? lt.Text : v.ToString();
                    var mapped = MapNameToSkillState(name);
                    if (mapped != null) return (int)mapped.Value;
                }
            }
            catch { }

            NodeId? vid = _currentStateNode;
            if (vid == null)
            {
                vid = await FindVariableNodeRecursive(session, BaseNodeId, "CurrentState") ?? await FindVariableNodeRecursive(session, BaseNodeId, "State");
            }
            if (vid == null) return null;
            try
            {
                var dv = await session.ReadValueAsync(vid, System.Threading.CancellationToken.None);
                var val = dv?.Value;
                if (val == null)
                {
                    // try fallback locations
                    try
                    {
                        NodeId? fallback = null;
                        if (_currentStateNode != null)
                        {
                            fallback = await FindVariableNodeRecursive(session, _currentStateNode, "StateNumber") ?? await FindVariableNodeRecursive(session, _currentStateNode, "State");
                        }
                        if (fallback == null && _stateMachineNode != null)
                        {
                            fallback = await FindVariableNodeRecursive(session, _stateMachineNode, "StateNumber") ?? await FindVariableNodeRecursive(session, _stateMachineNode, "State");
                        }
                        if (fallback != null)
                        {
                            var dv2 = await session.ReadValueAsync(fallback, System.Threading.CancellationToken.None);
                            var v2 = dv2?.Value;
                            if (v2 is uint u2) return (int)u2;
                            if (v2 is int ii) return ii;
                            if (v2 is ushort us2) return (int)us2;
                            if (v2 is short s2) return (int)s2;
                            var name2 = v2 is LocalizedText lt2 ? lt2.Text : v2?.ToString();
                            var mapped2 = MapNameToSkillState(name2);
                            if (mapped2 != null) return (int)mapped2.Value;
                        }
                        try
                        {
                            var node = await session.ReadNodeAsync(vid);
                            var disp = node?.DisplayName?.Text ?? node?.BrowseName?.Name;
                            if (!string.IsNullOrEmpty(disp))
                            {
                                try { var enumVal3 = (Common.SkillStates)Enum.Parse(typeof(Common.SkillStates), disp, true); return (int)enumVal3; } catch { }
                            }
                        }
                        catch { }
                    }
                    catch { }

                    return null;
                }
                if (val is int i) return i;
                if (val is uint ui) return (int)ui;
                if (val is short s) return (int)s;
                if (val is ushort us) return (int)us;
                var name = val is LocalizedText ltx ? ltx.Text : val.ToString();
                var mappedMain = MapNameToSkillState(name);
                if (mappedMain != null) return (int)mappedMain.Value;
            }
            catch { }
            return null;
        }

        /// <summary>
        /// Determine if skill is finite based on available states (contains 'Completed').
        /// Fallback to FinalResultData heuristic when available states are unknown.
        /// </summary>
        public bool IsFinite
        {
            get
            {
                if (_isFiniteHint.HasValue) return _isFiniteHint.Value;
                if (_availableStates != null && _availableStates.Count > 0)
                {
                    try { UAClient.Common.Log.Debug($"RemoteSkill '{Name}': AvailableStates count={_availableStates.Count}"); } catch { }
                    foreach (var s in _availableStates)
                    {
                        if (string.Equals(s, "Completed", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(s, "Completing", StringComparison.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                    }
                    return false;
                }
                try { UAClient.Common.Log.Debug($"RemoteSkill '{Name}': AvailableStates unknown, FinalResultData.Count={FinalResultData?.Count ?? 0}"); } catch { }
                return FinalResultData != null && FinalResultData.Count > 0;
            }
        }

        private async Task<NodeId?> FindNodeByBrowseNameRecursive(Session session, NodeId startNode, string browseName)
        {
            // Try translate-based fast paths
            try
            {
                var t1 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { browseName }); if (t1 != null) return t1;
                var t2 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { "SkillExecution", browseName }); if (t2 != null) return t2;
                var t3 = await SessionBrowseCache.TranslatePathAsync(session, startNode, new[] { "StateMachine", browseName }); if (t3 != null) return t3;
            }
            catch { }

            var browser = new Browser(session)
            {
                BrowseDirection = BrowseDirection.Forward,
                ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
            };
            var queue = new Queue<NodeId>(); queue.Enqueue(startNode);
            while (queue.Count > 0)
            {
                var node = queue.Dequeue();
                ReferenceDescriptionCollection refs = null;
                try { refs = await browser.BrowseAsync(node); } catch { }
                if (refs == null) continue;
                foreach (var r in refs)
                {
                    try
                    {
                        if (r?.NodeId == null) continue;
                        var name = r.DisplayName?.Text ?? r.BrowseName?.Name;
                        var expanded = r.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r.NodeId);
                        var childId = UaHelpers.ToNodeId(expanded, session);
                        if (childId == null) continue;
                        if (!string.IsNullOrEmpty(name) && string.Equals(name, browseName, StringComparison.OrdinalIgnoreCase)) return childId;
                        if (r.NodeClass == NodeClass.Object || r.NodeClass == NodeClass.Variable) queue.Enqueue(childId);
                    }
                    catch { }
                }
            }
            return null;
        }

        private async Task<NodeId?> GetTypeDefinitionAsync(Session session, NodeId nodeId)
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

        private async Task<string?> GetBrowseNameAsync(Session session, NodeId nodeId)
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

        public override async Task SetupSubscriptionsAsync(SubscriptionManager subscriptionManager, bool createSubscriptions = false)
        {
            // allow base to collect FinalResultData (and optionally create subscriptions)
            await base.SetupSubscriptionsAsync(subscriptionManager, createSubscriptions);
            // after base call, an internal subscription manager may have been created
            var subMgr = subscriptionManager ?? _internalSubscriptionManager;
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");

            // Locate SkillExecution -> StateMachine -> CurrentState / AvailableStates; avoid expensive reads/subscriptions unless requested
            try
            {
                var skillExec = await FindNodeByBrowseNameRecursive(session, BaseNodeId, "SkillExecution");
                if (skillExec == null) return;
                var stateMachine = await FindNodeByBrowseNameRecursive(session, skillExec, "StateMachine");
                if (stateMachine == null) return;

                _stateMachineNode = stateMachine;
                _currentStateNode = await FindNodeByBrowseNameRecursive(session, stateMachine, "CurrentState");

                var avail = await FindNodeByBrowseNameRecursive(session, stateMachine, "AvailableStates");
                if (avail != null)
                {
                    try
                    {
                        var dv = await session.ReadValueAsync(avail, System.Threading.CancellationToken.None);
                        var val = dv.Value;
                        var list = new List<string>();
                        if (val is string[] sa) list.AddRange(sa);
                        else if (val is object[] oa) foreach (var o in oa) list.Add(o?.ToString() ?? "");
                        else if (val != null) list.Add(val.ToString() ?? "");
                        if (list.Count > 0) _availableStates = list;
                    }
                    catch { }
                }

                // determine skill type from TypeDefinition if possible (cheap browse)
                try
                {
                    var typeDef = await GetTypeDefinitionAsync(session, BaseNodeId);
                    if (typeDef != null)
                    {
                        var typeName = await GetBrowseNameAsync(session, typeDef);
                        if (!string.IsNullOrEmpty(typeName))
                        {
                            if (string.Equals(typeName, "FiniteSkillType", StringComparison.OrdinalIgnoreCase)) _isFiniteHint = true;
                            else if (string.Equals(typeName, "ContinuousSkillType", StringComparison.OrdinalIgnoreCase)) _isFiniteHint = false;
                        }
                    }
                }
                catch { }

                // If caller requested subscription creation, ensure subscription manager exists and subscribe to CurrentState
                if (subMgr == null)
                {
                    try { subMgr = new SubscriptionManager(RemoteServerClient); _internalSubscriptionManager = subMgr; } catch { subMgr = null; }
                }

                if (_currentStateNode != null && subMgr != null)
                {
                    try
                    {
                        // create RemoteVariable wrapper if missing
                        lock (_monitoring_nodes)
                        {
                            if (!_monitoring_nodes.ContainsKey(_currentStateNode))
                            {
                                var rv = new RemoteVariable("CurrentState", _currentStateNode);
                                rv.SetClient(RemoteServerClient);
                                _monitoring_nodes[_currentStateNode] = rv;
                                lock (Monitoring) { if (!Monitoring.ContainsKey("CurrentState")) Monitoring["CurrentState"] = rv; }
                            }
                        }

                        await subMgr.SubscribeDataChangeAsync(this, new[] { _currentStateNode });

                        // initialize cached state from a read (best-effort)
                        try
                        {
                            var cv = await session.ReadValueAsync(_currentStateNode, System.Threading.CancellationToken.None);
                            if (cv?.Value != null)
                            {
                                var v = cv.Value;
                                if (v is int vi && Enum.IsDefined(typeof(Common.SkillStates), vi)) _cachedState = (Common.SkillStates)vi;
                                else if (v is uint vui) { var ii = (int)vui; if (Enum.IsDefined(typeof(Common.SkillStates), ii)) _cachedState = (Common.SkillStates)ii; }
                                else { var s = v is LocalizedText ltx ? ltx.Text : v.ToString(); if (!string.IsNullOrEmpty(s)) { try { _cachedState = (Common.SkillStates)Enum.Parse(typeof(Common.SkillStates), s, true); } catch { } } }
                            }
                        }
                        catch { }
                    }
                    catch { }
                }

                // Also attempt to discover Monitoring / ParameterSet / FinalResultData that may be placed under SkillElement/FunctionalGroup
                var groupNames = new[] { "Monitoring", "ParameterSet", "FinalResultData" };
                foreach (var gname in groupNames)
                {
                    try
                    {
                        var node = await FindNodeByBrowseNameRecursive(session, BaseNodeId, gname);
                        if (node == null && skillExec != null) node = await FindNodeByBrowseNameRecursive(session, skillExec, gname);
                        if (node == null && _stateMachineNode != null) node = await FindNodeByBrowseNameRecursive(session, _stateMachineNode, gname);
                        if (node == null) continue;

                        var nodeMap = new Dictionary<NodeId, RemoteVariable>();
                        var addTo = gname == "ParameterSet" ? ParameterSet : (gname == "Monitoring" ? Monitoring : FinalResultData);
                        bool parameterSetFlag = gname == "ParameterSet";
                        try { await RemoteVariableCollector.AddVariableNodesAsync(session, node, nodeMap, addTo, parameterSetFlag); } catch { }
                        foreach (var kv in nodeMap)
                        {
                            if (gname == "Monitoring") _monitoring_nodes[kv.Key] = kv.Value;
                            if (gname == "ParameterSet") _parameter_nodes[kv.Key] = kv.Value;
                        }
                        foreach (var v in addTo.Values) { try { v.SetClient(RemoteServerClient); } catch { } }

                        if (nodeMap.Count > 0 && createSubscriptions && subMgr != null)
                        {
                            try { await subMgr.SubscribeDataChangeAsync(this, nodeMap.Keys); } catch { }
                        }
                    }
                    catch { }
                }
            }
            catch { }
        }

        // Subscribe core monitoring variables (ensure CurrentState is subscribed even if not in Monitoring folder)
        public override async Task SubscribeCoreAsync(SubscriptionManager subscriptionManager)
        {
            if (subscriptionManager == null) return;
            // ensure base monitoring subscriptions (if any)
            await base.SubscribeCoreAsync(subscriptionManager);

            // If we already know the CurrentState node, ensure it's exposed as RemoteVariable and subscribed with this as handler
            try
            {
                var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
                if (_currentStateNode == null)
                {
                    // try to discover CurrentState under the state machine path
                    var potential = await FindVariableNodeRecursive(session, BaseNodeId, "CurrentState");
                    if (potential != null) _currentStateNode = potential;
                }

                if (_currentStateNode != null)
                {
                    lock (_monitoring_nodes)
                    {
                        if (!_monitoring_nodes.ContainsKey(_currentStateNode))
                        {
                            var rv = new RemoteVariable("CurrentState", _currentStateNode);
                            rv.SetClient(RemoteServerClient);
                            _monitoring_nodes[_currentStateNode] = rv;
                            lock (Monitoring)
                            {
                                if (!Monitoring.ContainsKey("CurrentState")) Monitoring["CurrentState"] = rv;
                            }
                        }
                        else
                        {
                            // ensure client set
                            try { _monitoring_nodes[_currentStateNode].SetClient(RemoteServerClient); } catch { }
                        }
                    }

                    try
                    {
                        await subscriptionManager.SubscribeDataChangeAsync(this, new[] { _currentStateNode });
                    }
                    catch
                    {
                        // fallback: subscribe the remote variable itself
                        try { await _monitoring_nodes[_currentStateNode].SetupSubscriptionAsync(subscriptionManager); } catch { }
                    }
                }
            }
            catch { }
        }

        public void AddStateSubscriber(Action<Common.SkillStates> callback)
        {
            if (callback == null) return;
            _stateSubscribers.Add(callback);
        }

        private void NotifyStateSubscribers(Common.SkillStates state)
        {
            foreach (var cb in _stateSubscribers)
            {
                try { cb(state); } catch { }
            }
        }

        public override void DataChangeNotification(NodeId nodeId, DataValue value, MonitoredItemNotificationEventArgs args)
        {
            try
            {
                if (_currentStateNode != null && nodeId != null && nodeId == _currentStateNode)
                {
                    // let base update monitoring variable mapping
                    base.DataChangeNotification(nodeId, value, args);

                    try
                    {
                        object? v = value?.Value;
                        Common.SkillStates? parsed = null;
                        if (v == null) { }
                        else if (v is int ii) { if (Enum.IsDefined(typeof(Common.SkillStates), ii)) parsed = (Common.SkillStates)ii; }
                        else if (v is uint uii) { var iival = (int)uii; if (Enum.IsDefined(typeof(Common.SkillStates), iival)) parsed = (Common.SkillStates)iival; }
                        else { var s = v is LocalizedText lt ? lt.Text : v.ToString(); if (!string.IsNullOrEmpty(s)) { parsed = MapNameToSkillState(s); } }

                        if (parsed != null && parsed.Value != _cachedState)
                        {
                            _cachedState = parsed.Value;
                            try { UAClient.Common.Log.Debug($"RemoteSkill '{Name}': state changed -> {_cachedState}"); } catch { }
                            NotifyStateSubscribers(_cachedState);
                        }
                    }
                    catch { }
                    return;
                }
            }
            catch { }

            base.DataChangeNotification(nodeId, value, args);
        }

        private Common.SkillStates? MapNameToSkillState(string? name)
        {
            if (string.IsNullOrEmpty(name)) return null;
            var s = name.Trim();
            // common server might include numeric prefix or parenthetical text, strip extras
            // e.g. "Ready", "running", "Running (1)", etc.
            var idx = s.IndexOf('(');
            if (idx >= 0) s = s.Substring(0, idx).Trim();
            // normalize known synonyms
            switch (s.ToLowerInvariant())
            {
                case "halted": return Common.SkillStates.Halted;
                case "ready": return Common.SkillStates.Ready;
                case "running": return Common.SkillStates.Running;
                case "suspended": return Common.SkillStates.Suspended;
                case "completed": return Common.SkillStates.Completed;
                case "starting": return Common.SkillStates.Starting;
                case "halting": return Common.SkillStates.Halting;
                case "completing": return Common.SkillStates.Completing;
                case "resetting": return Common.SkillStates.Resetting;
                case "suspending": return Common.SkillStates.Suspending;
                // sometimes servers use German or other terms or state numbers as text; try parse enum ignoring case
                default:
                    try { return (Common.SkillStates)Enum.Parse(typeof(Common.SkillStates), s, true); } catch { }
                    break;
            }
            return null;
        }

        private static bool IsTransitionState(Common.SkillStates state)
        {
            switch (state)
            {
                case Common.SkillStates.Starting:
                case Common.SkillStates.Halting:
                case Common.SkillStates.Completing:
                case Common.SkillStates.Resetting:
                case Common.SkillStates.Suspending:
                    return true;
                default:
                    return false;
            }
        }

        private static bool IsResetCandidate(Common.SkillStates state)
        {
            switch (state)
            {
                case Common.SkillStates.Halted:
                case Common.SkillStates.Completed:
                case Common.SkillStates.Suspended:
                    return true;
                default:
                    return false;
            }
        }

        private static Common.SkillStates? NormalizeState(int? stateValue)
        {
            if (!stateValue.HasValue) return null;
            if (Enum.IsDefined(typeof(Common.SkillStates), stateValue.Value)) return (Common.SkillStates)stateValue.Value;
            return null;
        }

        private async Task<Common.SkillStates?> WaitForStableStateAsync(Common.SkillStates? currentState, TimeSpan timeout)
        {
            if (!currentState.HasValue) return null;
            if (!IsTransitionState(currentState.Value)) return currentState;

            var sw = Stopwatch.StartNew();
            var state = currentState.Value;
            while (sw.Elapsed < timeout)
            {
                UAClient.Common.Log.Info($"RemoteSkill '{Name}': waiting for transition state {state} to settle");
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                var refreshed = NormalizeState(await GetStateAsync());
                if (!refreshed.HasValue) return null;
                state = refreshed.Value;
                if (!IsTransitionState(state)) return state;
            }

            UAClient.Common.Log.Warn($"RemoteSkill '{Name}': transition state {state} did not settle within {timeout}");
            return state;
        }

        private async Task<bool> WaitForStateWithSubscriptionAsync(Common.SkillStates desired, TimeSpan timeout)
        {
            // quick check
            if (_cachedState == desired) return true;
            try { await EnsureCoreSubscribedAsync(); } catch { }
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            Action<Common.SkillStates> cb = null!;
            cb = (s) =>
            {
                try
                {
                    if (s == desired) tcs.TrySetResult(true);
                }
                catch { }
            };
            AddStateSubscriber(cb);
            try
            {
                // also check current state in case it changed before subscriber added
                if (_cachedState == desired) return true;
                var completed = await Task.WhenAny(tcs.Task, Task.Delay(timeout));
                return completed == tcs.Task && await tcs.Task;
            }
            finally
            {
                try { _stateSubscribers.Remove(cb); } catch { }
            }
        }

        public async Task<bool> WaitForStateAsync(Common.SkillStates desired, TimeSpan timeout, bool preferSubscriptions = true)
        {
            if (preferSubscriptions)
            {
                try
                {
                    var ok = await WaitForStateWithSubscriptionAsync(desired, timeout);
                    if (ok) return true;
                }
                catch { }
            }

            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < timeout)
            {
                try
                {
                    var st = await GetStateAsync();
                    if (st != null && st == (int)desired) return true;
                }
                catch { }
                await Task.Delay(TimeSpan.FromMilliseconds(250));
            }
            return false;
        }

        public async Task SetInputParametersAsync(IDictionary<string, object?>? parameters, bool skipNullValues = false)
        {
            if (parameters == null || parameters.Count == 0) return;
            foreach (var kv in parameters)
            {
                if (skipNullValues && kv.Value == null) continue;
                
                // Log parameter details before writing to OPC UA
                var valueType = kv.Value?.GetType().Name ?? "null";
                var valueDisplay = kv.Value?.ToString() ?? "<null>";
                UAClient.Common.Log.Info($"RemoteSkill '{Name}': writing parameter '{kv.Key}' with type={valueType}, value={valueDisplay}");
                
                await WriteParameterAsync(kv.Key, kv.Value ?? string.Empty);
            }
        }

        public async Task<IDictionary<string, object?>> ReadInputParametersAsync(IEnumerable<string>? parameterNames = null, bool refresh = false)
        {
            await EnsureParameterSetAvailableAsync();
            var names = parameterNames?.ToList() ?? ParameterSet.Keys.ToList();
            var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            if (names.Count == 0) return result;

            Session? session = null;
            if (refresh)
            {
                session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            }

            foreach (var name in names)
            {
                if (!ParameterSet.TryGetValue(name, out var variable)) continue;
                if (refresh && session != null)
                {
                    await RefreshRemoteVariableAsync(variable, session);
                }
                result[name] = variable.Value;
            }

            return result;
        }

        public async Task<IDictionary<string, object?>> ReadMonitoringDataAsync(IEnumerable<string>? variableNames = null, bool refresh = false)
        {
            await EnsureMonitoringAvailableAsync(createSubscriptions: true);
            try { await EnsureCoreSubscribedAsync(); } catch { }

            var names = variableNames?.ToList() ?? Monitoring.Keys.ToList();
            var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            if (names.Count == 0) return result;

            Session? session = null;
            if (refresh)
            {
                session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            }

            foreach (var name in names)
            {
                if (!Monitoring.TryGetValue(name, out var variable)) continue;
                if (refresh && session != null)
                {
                    await RefreshRemoteVariableAsync(variable, session);
                }
                result[name] = variable.Value;
            }

            return result;
        }

        public async Task StartAsync(params object[] inputs)
        {
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            // Prefer cached Start method node id to avoid repeated searches
            NodeId? methodId = _startMethodNode;
            if (methodId == null)
            {
                methodId = await FindMethodNodeRecursive(session, BaseNodeId, "Start");
            }
            if (methodId == null) throw new InvalidOperationException("Start method not found for skill");
            UAClient.Common.Log.Debug($"RemoteSkill '{Name}': calling Start method (methodId={methodId}) on BaseNodeId={BaseNodeId}");
            try
            {
                var callRes = await session.CallAsync(BaseNodeId, methodId, System.Threading.CancellationToken.None, inputs);
                // start call result (verbose debug suppressed)
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"RemoteSkill '{Name}': Start call on BaseNodeId failed: {ex.Message}");
                throw;
            }

            // actively poll at ~20Hz for a short period to detect Running or Completed (prefer subscription-updated _cachedState)
            try
            {
                var sw = Stopwatch.StartNew();
                var pollInterval = TimeSpan.FromMilliseconds(50); // ~20Hz
                var pollTimeout = TimeSpan.FromSeconds(2);
                bool seenRunning = false, seenCompleted = false;
                while (sw.Elapsed < pollTimeout)
                {
                    var cs = _cachedState;
                    if (cs == Common.SkillStates.Running) { seenRunning = true; break; }
                    if (cs == Common.SkillStates.Completed) { seenCompleted = true; break; }
                    // fallback: occasionally try a read if subscription hasn't updated yet
                    if (sw.Elapsed > TimeSpan.FromMilliseconds(200))
                    {
                        try
                        {
                            var read = await GetStateAsync();
                            if (read == (int)Common.SkillStates.Running) { seenRunning = true; break; }
                            if (read == (int)Common.SkillStates.Completed) { seenCompleted = true; break; }
                        }
                        catch { }
                    }
                    await Task.Delay(pollInterval);
                }
                if (seenRunning) { UAClient.Common.Log.Debug($"RemoteSkill '{Name}': Start resulted in Running state"); return; }
                if (seenCompleted) { UAClient.Common.Log.Debug($"RemoteSkill '{Name}': Start resulted in Completed state"); return; }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"RemoteSkill '{Name}': polling after Start failed: {ex.Message}");
            }

            // Retry: if we have a dedicated state machine node, try calling Start there
            if (_stateMachineNode != null && !_stateMachineNode.Equals(BaseNodeId))
            {
                UAClient.Common.Log.Debug($"RemoteSkill '{Name}': Start did not change state, retrying Start on StateMachineNode={_stateMachineNode}");
                try
                {
                    var callRes2 = await session.CallAsync(_stateMachineNode, methodId, System.Threading.CancellationToken.None, inputs);
                    // start retry call result (verbose debug suppressed)

                    // brief wait again
                    var ok = await WaitForStateWithSubscriptionAsync(Common.SkillStates.Running, TimeSpan.FromSeconds(2)) || await WaitForStateWithSubscriptionAsync(Common.SkillStates.Completed, TimeSpan.FromSeconds(2));
                    UAClient.Common.Log.Debug($"RemoteSkill '{Name}': Start retry resulted in state change: {ok}");
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': Start retry on StateMachineNode failed: {ex.Message}");
                }
            }
        }

        public async Task HaltAsync()
        {
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            var methodId = _haltMethodNode ?? await FindMethodNodeRecursive(session, BaseNodeId, "Halt");
            if (methodId == null) return;
            await session.CallAsync(BaseNodeId, methodId, System.Threading.CancellationToken.None);
        }

        public async Task ResetAsync()
        {
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            var methodId = _resetMethodNode ?? await FindMethodNodeRecursive(session, BaseNodeId, "Reset");
            if (methodId == null) return;
            await session.CallAsync(BaseNodeId, methodId, System.Threading.CancellationToken.None);
        }

        public async Task<IDictionary<string, object?>> ExecuteAsync(IDictionary<string, object?>? parameters = null,
             bool waitForCompletion = true,
             bool resetAfterCompletion = true,
             bool resetBeforeIfHalted = true,
             TimeSpan? timeout = null)
        {
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            timeout ??= TimeSpan.FromSeconds(30);

            var sw = Stopwatch.StartNew();
            UAClient.Common.Log.Debug($"RemoteSkill '{Name}': ExecuteAsync start (waitForCompletion={waitForCompletion}, timeout={timeout})");

            // ensure lightweight discovery of skill nodes; create subscriptions when we will need them
            try
            {
                var needSubscriptions = waitForCompletion || (FinalResultData != null && FinalResultData.Count > 0);
                await SetupSubscriptionsAsync(_internalSubscriptionManager, needSubscriptions);
                UAClient.Common.Log.Debug($"RemoteSkill '{Name}': SetupSubscriptionsAsync completed (createdSubscriptions={needSubscriptions})");
            }
            catch (Exception ex) { UAClient.Common.Log.Warn($"RemoteSkill '{Name}': SetupSubscriptionsAsync failed: {ex.Message}"); }

            // ensure core subscriptions (CurrentState etc.) are present before executing
            try { await EnsureCoreSubscribedAsync(); UAClient.Common.Log.Debug($"RemoteSkill '{Name}': core subscribed={_coreSubscribed}"); } catch (Exception ex) { UAClient.Common.Log.Warn($"RemoteSkill '{Name}': EnsureCoreSubscribedAsync failed: {ex.Message}"); }

            // Write parameters
            if (parameters != null)
            {
                UAClient.Common.Log.Debug($"RemoteSkill '{Name}': writing {parameters.Count} parameters");
                foreach (var kv in parameters)
                {
                    try
                    {
                        // parameter write details suppressed to avoid verbose debug output
                        await WriteParameterAsync(kv.Key, kv.Value ?? "");
                    }
                    catch (Exception ex)
                    {
                        UAClient.Common.Log.Warn($"RemoteSkill '{Name}': WriteParameterAsync failed for {kv.Key}: {ex.Message}");
                    }
                }
            }

            // Check and handle pre-start states:
            bool resetDenied = false;
            bool skipStartDueToRunning = false;
            try
            {
                var normalizedState = NormalizeState(await GetStateAsync());
                normalizedState = await WaitForStableStateAsync(normalizedState, timeout.Value);

                if (normalizedState.HasValue && normalizedState.Value == Common.SkillStates.Running)
                {
                    if (IsFinite)
                    {
                        throw new InvalidOperationException($"RemoteSkill '{Name}' is already running (state={(int)normalizedState.Value})");
                    }

                    skipStartDueToRunning = true;
                    UAClient.Common.Log.Info($"RemoteSkill '{Name}': continuous skill already running; skipping Reset/Start");
                }
                else if (normalizedState.HasValue && IsResetCandidate(normalizedState.Value))
                {
                    if (resetBeforeIfHalted)
                    {
                        UAClient.Common.Log.Info($"RemoteSkill '{Name}': attempting Reset before start (state={(int)normalizedState.Value})");
                        try
                        {
                            var resetTarget = _stateMachineNode ?? BaseNodeId;
                            var resetMethod = _resetMethodNode ?? await FindMethodNodeRecursive(session, resetTarget, "Reset");
                            if (resetMethod == null)
                            {
                                UAClient.Common.Log.Warn($"RemoteSkill '{Name}': Reset method not found before start");
                            }
                            else
                            {
                                await session.CallAsync(resetTarget, resetMethod, System.Threading.CancellationToken.None);
                                var ok = await WaitForStateWithSubscriptionAsync(Common.SkillStates.Ready, timeout.Value);
                                if (ok)
                                {
                                    UAClient.Common.Log.Info($"RemoteSkill '{Name}': Reset before start succeeded, skill is Ready");
                                }
                                else
                                {
                                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': Reset before start did not reach Ready within timeout");
                                }
                            }
                        }
                        catch (ServiceResultException sre) when (sre.StatusCode == StatusCodes.BadUserAccessDenied)
                        {
                            resetDenied = true;
                            UAClient.Common.Log.Warn($"RemoteSkill '{Name}': Reset before start failed: {sre.StatusCode}");
                        }
                        catch (Exception ex)
                        {
                            UAClient.Common.Log.Warn($"RemoteSkill '{Name}': Reset before start failed: {ex.Message}");
                            throw;
                        }
                    }
                    else
                    {
                        UAClient.Common.Log.Info($"RemoteSkill '{Name}': not resetting before start (state={(int)normalizedState.Value}, resetBeforeIfHalted=false)");
                    }
                }
                else if (!normalizedState.HasValue)
                {
                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': unable to determine state before execution");
                }
                else if (IsTransitionState(normalizedState.Value))
                {
                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': skill remained in transition state {normalizedState.Value} before start");
                }
            }
            catch (InvalidOperationException)
            {
                throw;
            }
            catch
            {
                // swallow other errors to preserve previous behaviour
            }

            if (skipStartDueToRunning)
            {
                sw.Stop();
                return await ReadFinalResultSnapshotAsync(session);
            }

            var cur = await GetStateAsync();
            if (cur == null || cur != (int)Common.SkillStates.Ready)
            {
                if (!resetDenied)
                {
                    throw new InvalidOperationException($"RemoteSkill '{Name}' is not in Ready state (current={cur})");
                }
                else
                {
                    UAClient.Common.Log.Info($"RemoteSkill '{Name}': Reset was denied, attempting Start even though not Ready (current={cur})");
                }
            }

            UAClient.Common.Log.Debug($"RemoteSkill '{Name}': invoking Start");
            ServiceResultException? invalidStateException = null;
            try
            {
                await StartAsync();
                UAClient.Common.Log.Debug($"RemoteSkill '{Name}': Start invoked");
            }
            catch (ServiceResultException sre) when (sre.StatusCode == StatusCodes.BadInvalidState)
            {
                invalidStateException = sre;
                UAClient.Common.Log.Warn($"RemoteSkill '{Name}': StartAsync failed with BadInvalidState; attempting recovery");
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"RemoteSkill '{Name}': StartAsync failed: {ex.Message}");
                throw;
            }

            if (invalidStateException != null)
            {
                var recovered = await TryRecoverFromInvalidStateAsync(timeout.Value);
                if (!recovered)
                {
                    throw invalidStateException;
                }

                UAClient.Common.Log.Info($"RemoteSkill '{Name}': retrying Start after recovery");
                await StartAsync();
            }

            // Wait for Running state, but accept an immediate Completed as success (skill may be very short-lived)
            var runTimeout = TimeSpan.FromSeconds(10);
            var tRun = WaitForStateWithSubscriptionAsync(Common.SkillStates.Running, runTimeout);
            var tComp = WaitForStateWithSubscriptionAsync(Common.SkillStates.Completed, runTimeout);
            var first = await Task.WhenAny(tRun, tComp);
            var runningOk = false;
            var completedOk = false;
            if (first == tRun) runningOk = await tRun;
            else completedOk = await tComp;

            UAClient.Common.Log.Debug($"RemoteSkill '{Name}': awaited Running => {runningOk}, awaited Completed => {completedOk}");

            if (!runningOk && !completedOk)
            {
                try
                {
                    var cached = _cachedState;
                    var read = await GetStateAsync();
                    var csv = CurrentStateVariable?.Value;
                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': Running/Completed wait failed. cached={cached}, read={read}, csv={csv}");
                }
                catch { }
                throw new TimeoutException("Skill did not reach Running or Completed state after Start");
            }

            // If the skill already completed immediately after start, treat it as finished for the "waitForCompletion" case
            var alreadyCompletedAfterStart = completedOk || _cachedState == Common.SkillStates.Completed;

            if (!waitForCompletion) { UAClient.Common.Log.Info($"RemoteSkill '{Name}': not waiting for completion, returning early (short delay)"); await Task.Delay(500); }

            if (waitForCompletion)
            {
                UAClient.Common.Log.Info($"RemoteSkill '{Name}': waiting for completion (IsFinite={IsFinite})");
                if (IsFinite) await WaitForStateWithSubscriptionAsync(Common.SkillStates.Completed, timeout.Value);
                else await WaitForStateWithSubscriptionAsync(Common.SkillStates.Halted, timeout.Value);
                UAClient.Common.Log.Info($"RemoteSkill '{Name}': completion wait finished. CurrentState={_cachedState}");
            }

            // Prefer values populated by subscriptions; fall back to reads
            var outDict = await ReadFinalResultSnapshotAsync(session);

            if (resetAfterCompletion)
            {
                try
                {
                    var resetTarget = _stateMachineNode ?? BaseNodeId;
                    var resetMethod = _resetMethodNode ?? await FindMethodNodeRecursive(session, resetTarget, "Reset");
                    if (resetMethod != null)
                    {
                        UAClient.Common.Log.Info($"RemoteSkill '{Name}': attempting Reset after completion");
                        try
                        {
                            await session.CallAsync(resetTarget, resetMethod, System.Threading.CancellationToken.None);
                            var ok2 = await WaitForStateWithSubscriptionAsync(Common.SkillStates.Ready, timeout.Value);
                            if (ok2) UAClient.Common.Log.Info($"RemoteSkill '{Name}': Reset after completion succeeded, skill is Ready");
                            else UAClient.Common.Log.Warn($"RemoteSkill '{Name}': Reset after completion did not reach Ready within timeout");
                        }
                        catch (Exception ex)
                        {
                            UAClient.Common.Log.Warn($"RemoteSkill '{Name}': Reset after completion failed: {ex.Message}");
                        }
                    }
                }
                catch { }
            }

            sw.Stop();
            UAClient.Common.Log.Debug($"RemoteSkill '{Name}': ExecuteAsync finished in {sw.ElapsedMilliseconds} ms");
            return outDict;
        }

        public async Task<long?> GetSuccessfulExecutionsCountAsync()
        {
            await EnsureFinalResultDataAvailableAsync();
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            var snapshot = await ReadFinalResultSnapshotAsync(session);
            return TryExtractSuccessfulExecutionsCount(snapshot);
        }

        /// <summary>
        /// Reads FinalResultData and optionally waits for an expected SuccessfulExecutionsCount to be reached.
        /// </summary>
        public async Task<IDictionary<string, object?>?> ReadFinalResultDataAsync(TimeSpan? timeout = null, long? expectedSuccessfulExecutions = null)
        {
            await EnsureFinalResultDataAvailableAsync();
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            timeout ??= TimeSpan.FromSeconds(5);

            IDictionary<string, object?>? lastSnapshot = null;
            var sw = Stopwatch.StartNew();
            
            // If waiting for a specific count, log it for debugging
            if (expectedSuccessfulExecutions.HasValue)
            {
                UAClient.Common.Log.Debug($"RemoteSkill '{Name}': waiting for SuccessfulExecutionsCount >= {expectedSuccessfulExecutions.Value} (timeout={timeout.Value.TotalSeconds}s)");
            }
            
            while (sw.Elapsed < timeout.Value)
            {
                var snapshot = await ReadFinalResultSnapshotAsync(session);
                lastSnapshot = snapshot;
                
                if (snapshot != null && snapshot.Count > 0)
                {
                    if (!expectedSuccessfulExecutions.HasValue)
                    {
                        return snapshot;
                    }

                    var count = TryExtractSuccessfulExecutionsCount(snapshot);
                    if (count.HasValue)
                    {
                        if (count.Value >= expectedSuccessfulExecutions.Value)
                        {
                            UAClient.Common.Log.Debug($"RemoteSkill '{Name}': SuccessfulExecutionsCount reached {count.Value} (expected >= {expectedSuccessfulExecutions.Value}) after {sw.ElapsedMilliseconds}ms");
                            return snapshot;
                        }
                        else
                        {
                            UAClient.Common.Log.Debug($"RemoteSkill '{Name}': SuccessfulExecutionsCount is {count.Value}, waiting for >= {expectedSuccessfulExecutions.Value} (elapsed: {sw.ElapsedMilliseconds}ms)");
                        }
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100));
            }

            // Log warning if we timed out waiting for the expected count
            if (expectedSuccessfulExecutions.HasValue && lastSnapshot != null)
            {
                var finalCount = TryExtractSuccessfulExecutionsCount(lastSnapshot);
                UAClient.Common.Log.Warn($"RemoteSkill '{Name}': ReadFinalResultDataAsync timed out. Expected count >= {expectedSuccessfulExecutions.Value}, actual count = {finalCount?.ToString() ?? "null"}");
            }

            return lastSnapshot;
        }

        /// <summary>
        /// Public helper to capture a one-time snapshot of FinalResultData using the current session.
        /// Allows higher layers to control reset timing while still leveraging the client's snapshot logic.
        /// </summary>
        public async Task<IDictionary<string, object?>> ReadFinalResultDataSnapshotAsync()
        {
            var session = RemoteServerClient.Session ?? throw new InvalidOperationException("No session");
            return await ReadFinalResultSnapshotAsync(session);
        }

        private async Task<Dictionary<string, object?>> ReadFinalResultSnapshotAsync(Session session)
        {
            var outDict = new Dictionary<string, object?>();
            if (FinalResultData != null)
            {
                foreach (var kv in FinalResultData)
                {
                    try
                    {
                        var rv = kv.Value;
                        var node = rv?.NodeId;
                        if (node == null)
                        {
                            outDict[kv.Key] = rv?.Value;
                            continue;
                        }

                        var dv = await session.ReadValueAsync(node, System.Threading.CancellationToken.None);
                        outDict[kv.Key] = dv?.Value;
                        try { rv?.UpdateFromDataValue(dv); } catch { }
                    }
                    catch { outDict[kv.Key] = null; }
                }
                UAClient.Common.Log.Debug($"RemoteSkill '{Name}': Read FinalResultData keys={outDict.Count}");
            }
            return outDict;
        }

        private async Task<bool> TryRecoverFromInvalidStateAsync(TimeSpan timeout)
        {
            try
            {
                var session = RemoteServerClient.Session;
                if (session == null) return false;

                // Wait briefly if the server reports that the state machine is still in a running transition
                var current = await GetStateAsync();
                var waitSlice = TimeSpan.FromMilliseconds(Math.Clamp(timeout.TotalMilliseconds / 3, 500, 4000));
                if (current == (int)Common.SkillStates.Starting || current == (int)Common.SkillStates.Running || current == (int)Common.SkillStates.Completing)
                {
                    UAClient.Common.Log.Info($"RemoteSkill '{Name}': waiting for skill to leave running state before retrying Start (state={current})");
                    await WaitForStateWithSubscriptionAsync(Common.SkillStates.Completed, waitSlice);
                    await WaitForStateWithSubscriptionAsync(Common.SkillStates.Halted, waitSlice);
                }

                try
                {
                    await HaltAsync();
                    await WaitForStateWithSubscriptionAsync(Common.SkillStates.Halted, waitSlice);
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Debug($"RemoteSkill '{Name}': Halt during recovery failed: {ex.Message}");
                }

                try
                {
                    await ResetAsync();
                    var ready = await WaitForStateWithSubscriptionAsync(Common.SkillStates.Ready, timeout);
                    if (!ready)
                    {
                        UAClient.Common.Log.Warn($"RemoteSkill '{Name}': recovery reset did not reach Ready");
                    }
                    return ready;
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteSkill '{Name}': recovery reset failed: {ex.Message}");
                    return false;
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"RemoteSkill '{Name}': recovery from BadInvalidState failed: {ex.Message}");
                return false;
            }
        }

        private async Task EnsureParameterSetAvailableAsync()
        {
            if (ParameterSet.Count > 0) return;
            try { await SetupSubscriptionsAsync(_remoteServer?.SubscriptionManager, false); } catch { }
        }

        private async Task EnsureMonitoringAvailableAsync(bool createSubscriptions)
        {
            if (Monitoring.Count > 0) return;
            try { await SetupSubscriptionsAsync(_remoteServer?.SubscriptionManager, createSubscriptions); } catch { }
        }

        private async Task EnsureFinalResultDataAvailableAsync()
        {
            if (FinalResultData.Count > 0) return;
            try { await SetupSubscriptionsAsync(_remoteServer?.SubscriptionManager, false); } catch { }
        }

        private static async Task RefreshRemoteVariableAsync(RemoteVariable variable, Session session)
        {
            if (variable?.NodeId == null || session == null) return;
            try
            {
                var dv = await session.ReadValueAsync(variable.NodeId, System.Threading.CancellationToken.None);
                if (dv != null) variable.UpdateFromDataValue(dv);
            }
            catch { }
        }

        private static long? TryExtractSuccessfulExecutionsCount(IDictionary<string, object?>? snapshot)
        {
            if (snapshot == null || snapshot.Count == 0) return null;
            foreach (var kv in snapshot)
            {
                if (string.IsNullOrEmpty(kv.Key)) continue;
                if (kv.Key.IndexOf("SuccessfulExecutionsCount", StringComparison.OrdinalIgnoreCase) < 0) continue;
                if (TryConvertToInt64(kv.Value, out var value)) return value;
            }
            return null;
        }

        private static bool TryConvertToInt64(object? value, out long result)
        {
            switch (value)
            {
                case long l:
                    result = l;
                    return true;
                case int i:
                    result = i;
                    return true;
                case short s:
                    result = s;
                    return true;
                case byte b:
                    result = b;
                    return true;
                case sbyte sb:
                    result = sb;
                    return true;
                case uint ui:
                    result = Convert.ToInt64(ui, CultureInfo.InvariantCulture);
                    return true;
                case ushort us:
                    result = us;
                    return true;
                case ulong ul when ul <= long.MaxValue:
                    result = Convert.ToInt64(ul, CultureInfo.InvariantCulture);
                    return true;
                case float f when !float.IsNaN(f) && !float.IsInfinity(f):
                    result = Convert.ToInt64(f, CultureInfo.InvariantCulture);
                    return true;
                case double d when !double.IsNaN(d) && !double.IsInfinity(d):
                    result = Convert.ToInt64(d, CultureInfo.InvariantCulture);
                    return true;
                case decimal dec when dec <= long.MaxValue && dec >= long.MinValue:
                    result = Convert.ToInt64(dec, CultureInfo.InvariantCulture);
                    return true;
                case string s when long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed):
                    result = parsed;
                    return true;
            }

            try
            {
                if (value is IFormattable formattable)
                {
                    result = Convert.ToInt64(formattable, CultureInfo.InvariantCulture);
                    return true;
                }
            }
            catch { }

            result = 0;
            return false;
        }

        // Ensure core subscriptions are created and cached for future use
        private async Task EnsureCoreSubscribedAsync()
        {
            // fast path
            if (_coreSubscribed) return;
            // ensure only one creator
            lock (_coreSubLock)
            {
                if (_coreSubscribed) return;
                // mark tentatively to prevent re-entry; actual subscribe happens outside lock
            }

            SubscriptionManager? subMgr = _remoteServer?.SubscriptionManager ?? _internalSubscriptionManager;
            if (subMgr == null)
            {
                try
                {
                    subMgr = new SubscriptionManager(RemoteServerClient);
                    _internalSubscriptionManager = subMgr;
                }
                catch { subMgr = null; }
            }

            if (subMgr != null)
            {
                try
                {
                    await SubscribeCoreAsync(subMgr);
                }
                catch { }
            }

            // log initial state after subscribing
            try
            {
                var initial = await GetStateAsync();
                try { UAClient.Common.Log.Info($"RemoteSkill '{Name}': initial state after core subscribe = {initial}"); } catch { }
            }
            catch { }

            // finalize flag
            lock (_coreSubLock)
            {
                _coreSubscribed = true;
            }
        }
    }
}
