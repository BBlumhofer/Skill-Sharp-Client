using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;

namespace OpenSmi.Client;

/// <summary>Port of <c>client/remote_ua_object.py</c> and opensmi-core <c>BaseUaObject</c>: base of all remote objects.</summary>
public abstract class RemoteUaObject
{
    private readonly object _initGate = new(); // not System.Threading.Lock: net8.0 has none
    private readonly HashSet<ObjectPath> _aliases = [];
    private Task? _init;
    private ObjectPath? _path;

    /// <param name="segment">Last path segment, unique among the siblings; defaults to the sanitized name.</param>
    private protected RemoteUaObject(string name, NodeId? nodeId, RemoteServer server, RemoteUaObject? parent,
        IReadOnlyList<BrowseRule>? browseRules = null, string? segment = null)
    {
        Name = name;
        Segment = segment ?? PathSegments.Sanitize(name);
        ObjectPath.CheckSegment(Segment);
        Server = server;
        Parent = parent;
        BrowseRules = browseRules ?? [BrowseRule.Default];
        Logger = server.LoggerFactory.CreateLogger(GetType());
        NodeId = NodeId.Null;
        if (nodeId != null) SetNodeId(nodeId);
    }

    /// <summary>Name as on the server (its browse name); may be empty or contain <c>/</c> on servers we don't control.</summary>
    public string Name { get; }
    /// <summary>Last segment of <see cref="Path"/>, the key in the parent's collection.</summary>
    public string Segment { get; }
    /// <summary>
    /// Canonical path: the parent's path, the OpenSMI folder (Components, Resources, SkillSet, MethodSet, Users) and
    /// the segment, e.g. <c>/M1/Components/Gripper/SkillSet/Pick</c>.
    /// </summary>
    public ObjectPath Path => _path ??= BasePath / Segment;
    /// <summary>Further paths of this object, e.g. of a variable also listed in another group.</summary>
    public IReadOnlyCollection<ObjectPath> Aliases { get { lock (_aliases) return [.. _aliases]; } }
    public NodeId NodeId { get; private set; }
    public string UaType { get; private protected set; } = "UNKNOWN";
    public RemoteUaObject? Parent { get; }
    public RemoteServer Server { get; }
    public IReadOnlyList<BrowseRule> BrowseRules { get; }
    public bool IsInitialized => _init is { IsCompletedSuccessfully: true };
    /// <summary>Failures of child browses; a failed child is left out.</summary>
    public IReadOnlyList<Exception> InitExceptions => InitExceptionList;

    private protected List<Exception> InitExceptionList { get; } = [];
    private protected ILogger Logger { get; }
    private protected ISession Session => Server.RequireSession();

    /// <summary>OpenSMI folder between the parent and this object in the path; <c>null</c> for none.</summary>
    private protected virtual string? Folder => null;

    private protected virtual ObjectPath BasePath =>
        Folder is { } folder ? (Parent?.Path ?? ObjectPath.Root) / folder : Parent?.Path ?? ObjectPath.Root;

    /// <summary>Idempotent: concurrent and repeated calls share one browse.</summary>
    public Task InitAsync(CancellationToken cancellationToken = default)
    {
        lock (_initGate) return _init ??= InitCoreAsync(cancellationToken);
    }

    public virtual async Task<string> ReadUaTypeAsync(CancellationToken cancellationToken = default) =>
        UaType = await UaUtil.ReadTypeNameAsync(Session, NodeId, cancellationToken).ConfigureAwait(false);

    /// <summary>The object at <paramref name="relative"/> below this one, e.g. <c>"Components/Gripper"</c>.</summary>
    /// <exception cref="KeyNotFoundException">No object has this path.</exception>
    /// <exception cref="ArgumentException"><paramref name="relative"/> starts with <c>/</c> or is no valid path.</exception>
    public RemoteUaObject Resolve(string relative) => Server.Resolve(Absolute(relative));

    public RemoteUaObject? ResolveOptional(string relative) => Server.ResolveOptional(Absolute(relative));

    /// <summary>All objects below this one whose path matches the relative glob, e.g. <c>"**/SkillSet/*"</c>.</summary>
    public IReadOnlyList<RemoteUaObject> Find(string pattern)
    {
        CheckRelative(pattern);
        var prefix = string.Concat(Path.Segments.Select(s => "/" + ObjectPath.Escape(s)));
        return [.. Server.Find($"{prefix}/{pattern}").Where(o => !ReferenceEquals(o, this))];
    }

    private ObjectPath Absolute(string relative)
    {
        CheckRelative(relative);
        return new ObjectPath([.. Path.Segments, .. relative.Split('/')]);
    }

    private static void CheckRelative(string relative)
    {
        if (relative.StartsWith('/'))
            throw new ArgumentException($"Expected a path relative to this object, got an absolute one: '{relative}'", nameof(relative));
    }

    internal void AddAlias(ObjectPath path)
    {
        lock (_aliases) _aliases.Add(path);
        Server.RegisterPath(path, this);
    }

    private protected abstract Task InitCoreAsync(CancellationToken ct);

    /// <summary>Registers this object by node and path; parents register first, so the path is fixed from now on.</summary>
    private protected void SetNodeId(NodeId nodeId)
    {
        if (!NodeId.IsNull(NodeId)) throw new InvalidOperationException($"{Path} already bound to {NodeId}");
        NodeId = nodeId;
        Server.Register(this);
    }

    /// <summary>Runs sub-browses concurrently; ordinary failures land in <see cref="InitExceptions"/>.</summary>
    private protected async Task GatherAsync(IEnumerable<(string Label, Func<Task> Run)> work)
    {
        var failures = await UaUtil.GatherAsync(work).ConfigureAwait(false);
        lock (InitExceptionList) InitExceptionList.AddRange(failures);
    }

    /// <summary>Inits the children concurrently and returns the successful ones in their original order.</summary>
    private protected async Task<List<T>> InitChildrenAsync<T>(List<T> children, CancellationToken ct) where T : RemoteUaObject
    {
        var failures = await UaUtil.InitAllAsync(children, ct).ConfigureAwait(false);
        lock (InitExceptionList) InitExceptionList.AddRange(failures);
        return [.. children.Where(c => c.IsInitialized)];
    }

    private protected Dictionary<NodeId, string> UniqueSegments(IEnumerable<ReferenceDescription> children) =>
        UaUtil.UniqueSegments(Session, children, Logger);

    /// <summary>
    /// Children of <paramref name="node"/> of <paramref name="nodeClass"/> with their segments. A child listed in several
    /// places belongs to the one referencing it with HasComponent (or another non-Organizes reference); places
    /// referencing it with Organizes only list it, as an alias. If several places reference it with HasComponent, the
    /// first in NodeId order owns it, with a warning.
    /// </summary>
    private protected async Task<List<(NodeId Node, string Name, string Segment, bool Owned)>> BrowseOwnedChildrenAsync(
        NodeId node, NodeClass nodeClass, CancellationToken ct)
    {
        var refs = (await UaUtil.BrowseAsync(Session, [node], ReferenceTypeIds.HierarchicalReferences, BrowseDirection.Forward,
            (uint)nodeClass, ct).ConfigureAwait(false))[0];
        var segments = UniqueSegments(refs);
        var candidates = refs.Where(r => r.ReferenceTypeId != ReferenceTypeIds.Organizes)
            .Select(r => UaUtil.ToNodeId(Session, r.NodeId)).Distinct().ToList();
        var owners = await AmbiguousOwnersAsync(candidates, ct).ConfigureAwait(false);

        var children = new List<(NodeId, string, string, bool)>();
        foreach (var r in refs)
        {
            var id = UaUtil.ToNodeId(Session, r.NodeId);
            if (children.Any(c => c.Item1 == id)) continue; // several references to the same child
            var owned = candidates.Contains(id) && (!owners.TryGetValue(id, out var owner) || owner == node);
            if (owned && owners.ContainsKey(id))
                Logger.LogWarning("Node {NodeId} has several owners (HasComponent), chose the first in NodeId order: {Path}",
                    id, Path / segments[id]);
            children.Add((id, r.BrowseName.Name ?? "", segments[id], owned));
        }
        return children;
    }

    /// <summary>The owner of each node referenced with HasComponent by more than one node: the first in NodeId order.</summary>
    private async Task<Dictionary<NodeId, NodeId>> AmbiguousOwnersAsync(List<NodeId> nodes, CancellationToken ct)
    {
        if (nodes.Count == 0) return [];
        var parents = await UaUtil.BrowseAsync(Session, nodes, ReferenceTypeIds.HasComponent, BrowseDirection.Inverse, 0, ct)
            .ConfigureAwait(false);
        var owners = new Dictionary<NodeId, NodeId>();
        for (var i = 0; i < nodes.Count; i++)
        {
            var ids = parents[i].Select(p => UaUtil.ToNodeId(Session, p.NodeId)).Distinct().ToList();
            if (ids.Count > 1) owners[nodes[i]] = ids.MinBy(p => UaUtil.ToKey(Session, p))!;
        }
        return owners;
    }

    public override string ToString() => $"{GetType().Name}({Path})";
}
