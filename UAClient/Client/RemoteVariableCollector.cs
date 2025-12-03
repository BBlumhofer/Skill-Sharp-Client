using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;

namespace UAClient.Client
{
    public static class RemoteVariableCollector
    {
        public static async System.Threading.Tasks.Task AddVariableNodesAsync(Session session, NodeId startNode, IDictionary<NodeId, RemoteVariable> nodeMapping, IDictionary<string, RemoteVariable> nameMapping, bool isWritable, int maxNodes = 5000)
        {
            var toVisit = new Queue<NodeId>();
            var visited = new HashSet<NodeId>();
            toVisit.Enqueue(startNode);
            visited.Add(startNode);
            var browser = new Browser(session)
            {
                BrowseDirection = BrowseDirection.Forward,
                ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                NodeClassMask = (int)(NodeClass.Object | NodeClass.Variable | NodeClass.Method)
            };
            int added = 0, skippedNonVariable = 0, failed = 0;
            var readParallelism = 20; // number of concurrent read requests
            var readSemaphore = new System.Threading.SemaphoreSlim(readParallelism);

            while (toVisit.Count > 0)
            {
                if (visited.Count >= maxNodes)
                {
                    UAClient.Common.Log.Warn($"RemoteVariableCollector: reached maxNodes limit ({maxNodes}), stopping traversal");
                    break;
                }
                var node = toVisit.Dequeue();
                ReferenceDescriptionCollection refs = null;
                try { refs = await browser.BrowseAsync(node); } catch (Exception ex) { UAClient.Common.Log.Debug($"BrowseAsync failed for {node}: {ex.Message}"); }
                if (refs == null) continue;

                var readTasks = new System.Collections.Generic.List<System.Threading.Tasks.Task>();

                foreach (var r in refs)
                {
                    try
                    {
                        if (r?.NodeId == null) continue;
                        if (r == null) continue;
                        var expanded = r.NodeId as ExpandedNodeId ?? new ExpandedNodeId(r.NodeId);
                        var childId = UaHelpers.ToNodeId(expanded, session);
                        if (childId == null) continue;
                        if (visited.Contains(childId)) continue;
                        visited.Add(childId);
                        var name = r.DisplayName?.Text ?? childId.ToString();

                        // Only log when the node is a Variable (reduces log volume)
                        if (r.NodeClass == NodeClass.Variable)
                        {
                            UAClient.Common.Log.Debug($"RemoteVariableCollector: found variable {name} ({childId})");
                        }

                        if (r.NodeClass == NodeClass.Variable)
                        {
                            // Skip well-known non-value properties that are not parameters/monitoring values
                            // (e.g. EURange, EngineeringUnits, EUInformation)
                            var excludedNames = new[] { "EURange", "EngineeringUnits", "EUInformation", "InstrumentRange", "EngineeringUnitsEU", "EURange" };
                            if (!string.IsNullOrEmpty(name) && excludedNames.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
                            {
                                UAClient.Common.Log.Debug($"RemoteVariableCollector: skipping property-like variable '{name}' ({childId})");
                                skippedNonVariable++;
                                // still enqueue children in case there are nested real variables, but do not treat this as a RemoteVariable
                                if (r.NodeClass == NodeClass.Object || r.NodeClass == NodeClass.Variable)
                                {
                                    toVisit.Enqueue(childId);
                                }
                                continue;
                            }

                            // schedule parallel read with bounded concurrency
                            var rv = new RemoteVariable(name, childId) { IsWritable = isWritable };
                            var task = System.Threading.Tasks.Task.Run(async () =>
                            {
                                await readSemaphore.WaitAsync();
                                try
                                {
                                    try
                                    {
                                        var dv = await session.ReadValueAsync(childId, System.Threading.CancellationToken.None);
                                        rv.UpdateFromDataValue(dv);
                                        System.Threading.Interlocked.Increment(ref added);
                                    }
                                    catch (Exception ex)
                                    {
                                        UAClient.Common.Log.Warn($"ReadValueAsync failed for {childId}: {ex.Message}");
                                        System.Threading.Interlocked.Increment(ref failed);
                                    }

                                    // protect concurrent writes to the provided dictionaries
                                    try
                                    {
                                        lock (nodeMapping)
                                        {
                                            nodeMapping[childId] = rv;
                                        }
                                    }
                                    catch { }

                                    if (!string.IsNullOrEmpty(rv.Name))
                                    {
                                        try
                                        {
                                            lock (nameMapping)
                                            {
                                                if (!nameMapping.ContainsKey(rv.Name))
                                                {
                                                    nameMapping[rv.Name] = rv;
                                                }
                                            }
                                        }
                                        catch { }
                                    }
                                }
                                finally { readSemaphore.Release(); }
                            });
                            readTasks.Add(task);
                        }
                        else
                        {
                            // not a variable — track as skipped for diagnostics
                            skippedNonVariable++;
                        }

                        // enqueue objects and variables for deeper traversal; methods generally don't have children
                        if (r.NodeClass == NodeClass.Object || r.NodeClass == NodeClass.Variable)
                        {
                            toVisit.Enqueue(childId);
                        }
                    }
                    catch (Exception ex)
                    {
                        UAClient.Common.Log.Warn($"RemoteVariableCollector: exception while processing child: {ex.Message}");
                    }
                }

                if (readTasks.Count > 0)
                {
                    try { await System.Threading.Tasks.Task.WhenAll(readTasks); } catch { }
                }
            }
            UAClient.Common.Log.Info($"RemoteVariableCollector: traversal complete. variables-added={added}, skipped-non-variable={skippedNonVariable}, read-failures={failed}, total-visited={visited.Count}");
        }
    }
}
