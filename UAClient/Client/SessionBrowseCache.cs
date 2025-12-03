using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using UAClient.Common;

namespace UAClient.Client
{
    /// <summary>
    /// Lightweight session-scoped cache and TranslateBrowsePaths helper.
    /// Caches resolved paths to NodeIds to avoid repeated Translate/Browse RPCs.
    /// </summary>
    public static class SessionBrowseCache
    {
        // key: sessionId|startNode|path (joined with '/') -> NodeId.ToString()
        private static readonly ConcurrentDictionary<string, string> _cache = new();

        private static string Key(Session session, NodeId start, string[] path)
        {
            var sid = session?.SessionId?.ToString() ?? "no-session";
            var p = string.Join("/", path.Select(s => s ?? ""));
            return sid + "|" + start.ToString() + "|" + p;
        }

        public static bool TryGet(Session session, NodeId start, string[] path, out NodeId? nodeId)
        {
            nodeId = null;
            var key = Key(session, start, path);
            if (_cache.TryGetValue(key, out var n))
            {
                nodeId = NodeId.Parse(n);
                return true;
            }
            return false;
        }

        public static void Store(Session session, NodeId start, string[] path, NodeId nodeId)
        {
            var key = Key(session, start, path);
            _cache[key] = nodeId.ToString();
        }

        /// <summary>
        /// Attempt to translate a simple relative path (array of browse names) using TranslateBrowsePathsToNodeIds.
        /// Tries with ReferenceType HasComponent first, then falls back to HierarchicalReferences.
        /// On success stores value in cache and returns NodeId.
        /// </summary>
        public static async Task<NodeId?> TranslatePathAsync(Session session, NodeId start, string[] path)
        {
            if (session == null) return null;
            if (path == null || path.Length == 0) return null;

            if (TryGet(session, start, path, out var cached)) return cached;

            // build BrowsePath
            BrowsePath bp = new BrowsePath { StartingNode = start };
            var elements = new RelativePathElementCollection();
            foreach (var p in path)
            {
                elements.Add(new RelativePathElement {
                    ReferenceTypeId = ReferenceTypeIds.HasComponent,
                    IsInverse = false,
                    IncludeSubtypes = true,
                    TargetName = new QualifiedName(p)
                });
            }
            bp.RelativePath = new RelativePath { Elements = elements };

            var bpc = new BrowsePathCollection { bp };
            try
            {
                // Prefer SDK async Translate API when available. Different SDK versions expose
                // different return types, so use reflection to extract the BrowsePathResultCollection
                // and DiagnosticInfoCollection if present.
                object resp = await session.TranslateBrowsePathsToNodeIdsAsync(null, bpc, System.Threading.CancellationToken.None).ConfigureAwait(false);

                BrowsePathResultCollection? browseResults = null;
                DiagnosticInfoCollection? diags = null;

                if (resp is BrowsePathResultCollection brc)
                {
                    browseResults = brc;
                }
                else if (resp != null)
                {
                    try
                    {
                        var t = resp.GetType();
                        var resultsProp = t.GetProperty("Results") ?? t.GetProperty("BrowsePathResults") ?? t.GetProperty("BrowseResults");
                        var diagsProp = t.GetProperty("DiagnosticInfos") ?? t.GetProperty("Diagnostics");
                        if (resultsProp != null)
                        {
                            browseResults = resultsProp.GetValue(resp) as BrowsePathResultCollection;
                        }
                        if (diagsProp != null)
                        {
                            diags = diagsProp.GetValue(resp) as DiagnosticInfoCollection;
                        }
                    }
                    catch { }
                }

                if (diags != null && diags.Count > 0)
                {
                    try { Log.Debug($"Translate fast-path diagnostics: {string.Join(";", diags.Select(d => d?.ToString() ?? "<null>"))}"); } catch { }
                }
                if (browseResults != null && browseResults.Count > 0 && browseResults[0].Targets != null && browseResults[0].Targets.Count > 0)
                {
                    var tgt = browseResults[0].Targets[0];
                    var nid = (NodeId)tgt.TargetId;
                    Store(session, start, path, nid);
                    return nid;
                }
            }
            catch
            {
                // ignore and try fallback
            }

            // fallback: try with HierarchicalReferences
            try
            {
                BrowsePath bp2 = new BrowsePath { StartingNode = start };
                var elements2 = new RelativePathElementCollection();
                foreach (var p in path)
                {
                    elements2.Add(new RelativePathElement {
                        ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                        IsInverse = false,
                        IncludeSubtypes = true,
                        TargetName = new QualifiedName(p)
                    });
                }
                bp2.RelativePath = new RelativePath { Elements = elements2 };
                var bpc2 = new BrowsePathCollection { bp2 };
                object resp2 = await session.TranslateBrowsePathsToNodeIdsAsync(null, bpc2, System.Threading.CancellationToken.None).ConfigureAwait(false);

                BrowsePathResultCollection? browseResults2 = null;
                DiagnosticInfoCollection? diags2 = null;

                if (resp2 is BrowsePathResultCollection brc2)
                {
                    browseResults2 = brc2;
                }
                else if (resp2 != null)
                {
                    try
                    {
                        var t = resp2.GetType();
                        var resultsProp = t.GetProperty("Results") ?? t.GetProperty("BrowsePathResults") ?? t.GetProperty("BrowseResults");
                        var diagsProp = t.GetProperty("DiagnosticInfos") ?? t.GetProperty("Diagnostics");
                        if (resultsProp != null)
                        {
                            browseResults2 = resultsProp.GetValue(resp2) as BrowsePathResultCollection;
                        }
                        if (diagsProp != null)
                        {
                            diags2 = diagsProp.GetValue(resp2) as DiagnosticInfoCollection;
                        }
                    }
                    catch { }
                }

                if (diags2 != null && diags2.Count > 0)
                {
                    try { Log.Debug($"Translate fallback diagnostics: {string.Join(";", diags2.Select(d => d?.ToString() ?? "<null>"))}"); } catch { }
                }
                if (browseResults2 != null && browseResults2.Count > 0 && browseResults2[0].Targets != null && browseResults2[0].Targets.Count > 0)
                {
                    var tgt = browseResults2[0].Targets[0];
                    var nid = (NodeId)tgt.TargetId;
                    Store(session, start, path, nid);
                    return nid;
                }
            }
            catch
            {
            }

            return null;
        }
    }
}
