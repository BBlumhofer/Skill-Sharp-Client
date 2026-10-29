using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;

namespace OpenSmi.Client;

/// <summary>Port of <c>client/remote_ua_object.py</c>: base of all remote objects.</summary>
public abstract class RemoteUaObject
{
    private readonly Lock _initGate = new();
    private Task? _init;

    private protected RemoteUaObject(string name, NodeId? nodeId, RemoteServer server, RemoteUaObject? parent,
        IReadOnlyList<BrowseRule>? browseRules = null)
    {
        Name = name;
        Server = server;
        Parent = parent;
        BrowseRules = browseRules ?? [BrowseRule.Default];
        Logger = server.LoggerFactory.CreateLogger(GetType());
        NodeId = NodeId.Null;
        if (nodeId != null) SetNodeId(nodeId);
    }

    public string Name { get; }
    /// <summary>POSIX path; folders such as Components or SkillSet are not segments.</summary>
    public string Path => Parent == null ? "/" + Name : Parent.Path + "/" + Name;
    public NodeId NodeId { get; private set; }
    public string UaType { get; private protected set; } = "UNKNOWN";
    public RemoteUaObject? Parent { get; }
    public RemoteServer Server { get; }
    public IReadOnlyList<BrowseRule> BrowseRules { get; }
    public bool IsInitialized => _init is { IsCompletedSuccessfully: true };
    /// <summary>Non-fatal failures of child browses; the failed child is simply missing.</summary>
    public IReadOnlyList<Exception> InitExceptions => InitExceptionList;

    private protected List<Exception> InitExceptionList { get; } = [];
    private protected ILogger Logger { get; }
    private protected ISession Session => Server.RequireSession();

    /// <summary>Idempotent: concurrent and repeated calls share one browse.</summary>
    public Task InitAsync(CancellationToken cancellationToken = default)
    {
        lock (_initGate) return _init ??= InitCoreAsync(cancellationToken);
    }

    public virtual async Task<string> ReadUaTypeAsync(CancellationToken cancellationToken = default) =>
        UaType = await UaUtil.ReadTypeNameAsync(Session, NodeId, cancellationToken).ConfigureAwait(false);

    private protected abstract Task InitCoreAsync(CancellationToken ct);

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

    public override string ToString() => $"{GetType().Name}({Path})";
}
