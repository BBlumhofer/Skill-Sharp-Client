using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace OpenSmi.Client;

/// <summary>Event from a component's <c>Notification</c> node. Alerts carry an error code.</summary>
public sealed record NotificationEvent(string Message, int Severity, string ErrorCode)
{
    /// <summary>
    /// The server sends alerts as <c>"&lt;msg&gt;|||&lt;code&gt;"</c>; the event's own ErrorCode wins over the suffix.
    /// Returns <c>null</c> for events without a message (Python drops them, too).
    /// </summary>
    public static NotificationEvent? Parse(string? message, int severity, string? errorCode)
    {
        if (message == null) return null;
        var parts = message.Split("|||", 2);
        return new(parts[0], severity, !string.IsNullOrEmpty(errorCode) ? errorCode : parts.Length > 1 ? parts[1] : "");
    }
}

/// <summary>Port of <c>BaseRemoteComponent</c>: a server-side BaseMachineryItem.</summary>
public abstract class BaseRemoteComponent : RemoteUaObject
{
    private Dictionary<string, RemoteSkill> _skillSet = [], _feasibilityCheckSet = [], _preconditionCheckSet = [];
    private Dictionary<string, RemoteMethod> _methodSet = [];
    private Dictionary<string, RemoteComponent> _components = [];
    private Dictionary<string, RemoteResource> _resources = [];

    private protected BaseRemoteComponent(string name, NodeId nodeId, RemoteServer server, RemoteUaObject? parent,
        IReadOnlyList<BrowseRule>? browseRules) : base(name, nodeId, server, parent, browseRules)
    {
        Identification = new(server, this);
        Monitoring = new(server, this);
        Attributes = new(server, this);
        ParameterSet = new(server, this);
        StatusChanged = new(Logger);
        NotificationReceived = new(Logger);
    }

    public IReadOnlyDictionary<string, RemoteSkill> SkillSet => _skillSet;
    public IReadOnlyDictionary<string, RemoteSkill> FeasibilityCheckSet => _feasibilityCheckSet;
    public IReadOnlyDictionary<string, RemoteSkill> PreconditionCheckSet => _preconditionCheckSet;
    public IReadOnlyDictionary<string, RemoteMethod> MethodSet => _methodSet;
    public IReadOnlyDictionary<string, RemoteComponent> Components => _components;
    public IReadOnlyDictionary<string, RemoteResource> Resources => _resources;
    public Identification Identification { get; }
    public Monitoring Monitoring { get; }
    public Attributes Attributes { get; }
    public ParameterSet ParameterSet { get; }
    /// <summary>Cached MachineryItemState text, "Unknown" until known.</summary>
    public string CurrentState { get; private set; } = "Unknown";
    public AsyncSignal<(BaseRemoteComponent Sender, string State)> StatusChanged { get; }
    public AsyncSignal<(BaseRemoteComponent Sender, NotificationEvent Event)> NotificationReceived { get; }

    internal RemoteLock? LockOrNull { get; private set; }
    private NodeId? _currentStateNode;

    /// <summary>Reads MachineryItemState/CurrentState as text, "Unknown" if the node is absent.</summary>
    public async Task<string> ReadCurrentStateAsync(CancellationToken cancellationToken = default)
    {
        if (_currentStateNode == null) return "Unknown";
        var dv = (await UaUtil.ReadAsync(Session, [(_currentStateNode, Opc.Ua.Attributes.Value)], cancellationToken).ConfigureAwait(false))[0];
        OpenSmiStatusException.ThrowIfBad(dv.StatusCode, $"Read of {Path} current state");
        return StateText(dv.Value);
    }

    private protected override async Task InitCoreAsync(CancellationToken ct)
    {
        var features = OpenSmi.Client.BrowseRules.Evaluate(BrowseRules, Path);
        var children = await UaUtil.BrowseChildrenAsync(Session, NodeId, ct).ConfigureAwait(false);
        var work = new List<(string, Func<Task>)>();
        foreach (var child in children)
        {
            var id = UaUtil.ToNodeId(Session, child.NodeId);
            var name = child.BrowseName.Name;
            Func<Task>? run = name switch
            {
                "Lock" when features.HasFlag(BrowseFeature.Lock) => () => InitLockAsync(id, ct),
                "SkillSet" when features.HasFlag(BrowseFeature.SkillSet) => () => BrowseSkillSetAsync(id, ct),
                // shortcut: no fallback to the legacy "Messages" child (v4 servers); add it with the SmartFactory package.
                "Notification" => () => Server.Subscriptions.SubscribeEventAsync(id, OnEventAsync, ct),
                "Identification" when features.HasFlag(BrowseFeature.Identification) => () => Identification.InitAsync(id, ct),
                "Attributes" when features.HasFlag(BrowseFeature.Attributes) => () => Attributes.InitAsync(id, ct),
                "Monitoring" when features.HasFlag(BrowseFeature.Monitoring) => () => Monitoring.InitAsync(id, ct),
                "ParameterSet" when features.HasFlag(BrowseFeature.ParameterSet) => () => ParameterSet.InitAsync(id, ct),
                "Components" when features.HasFlag(BrowseFeature.Components) => async () =>
                    _components = await BrowseFolderAsync(id, (n, i) => new RemoteComponent(n, i, Server, this, BrowseRules), ct).ConfigureAwait(false),
                "Resources" when features.HasFlag(BrowseFeature.Resources) => async () =>
                    _resources = await BrowseFolderAsync(id, (n, i) => new RemoteResource(n, i, Server, this, BrowseRules), ct).ConfigureAwait(false),
                "MethodSet" when features.HasFlag(BrowseFeature.MethodSet) => async () =>
                    _methodSet = await BrowseFolderAsync(id, (n, i) => new RemoteMethod(n, i, Server, this, BrowseRules), ct).ConfigureAwait(false),
                "MachineryBuildingBlocks" => () => BrowseBuildingBlocksAsync(id, features, ct),
                "Lock" or "SkillSet" or "Identification" or "Attributes" or "Monitoring" or "ParameterSet"
                    or "Components" or "Resources" or "MethodSet" => null,
                _ => Unsupported(name),
            };
            if (run != null) work.Add(($"{Path}/{name}", run));
        }
        await GatherAsync(work).ConfigureAwait(false);

        await Server.Subscriptions.SubscribeDataChangeAsync(
            new RemoteVariableContainer[] { Monitoring, Attributes, ParameterSet, Identification }.SelectMany(c => c.SubscriptionItems),
            ct).ConfigureAwait(false);
    }

    private Func<Task>? Unsupported(string name)
    {
        Logger.LogWarning("Unsupported node {Name} below {Path}", name, Path);
        return null;
    }

    /// <summary>Hook for machine-only building blocks (Users).</summary>
    private protected virtual Task BrowseBuildingBlockAsync(string name, NodeId nodeId, BrowseFeature features, CancellationToken ct) =>
        Task.CompletedTask;

    private async Task BrowseBuildingBlocksAsync(NodeId nodeId, BrowseFeature features, CancellationToken ct)
    {
        foreach (var child in await UaUtil.BrowseChildrenAsync(Session, nodeId, ct).ConfigureAwait(false))
        {
            var id = UaUtil.ToNodeId(Session, child.NodeId);
            if (child.BrowseName.Name != "MachineryItemState")
            {
                await BrowseBuildingBlockAsync(child.BrowseName.Name, id, features, ct).ConfigureAwait(false);
                continue;
            }
            var state = (await UaUtil.BrowseChildrenAsync(Session, id, ct).ConfigureAwait(false)).Child("CurrentState");
            if (state == null) continue;
            _currentStateNode = UaUtil.ToNodeId(Session, state.NodeId);
            await Server.Subscriptions.SubscribeDataChangeAsync([(_currentStateNode, OnCurrentStateAsync)], ct).ConfigureAwait(false);
        }
    }

    private async Task InitLockAsync(NodeId nodeId, CancellationToken ct)
    {
        var remoteLock = new RemoteLock(nodeId, Server, this);
        await remoteLock.InitAsync(ct).ConfigureAwait(false);
        await remoteLock.ReadUaTypeAsync(ct).ConfigureAwait(false);
        LockOrNull = remoteLock;
    }

    private async Task<Dictionary<string, T>> BrowseFolderAsync<T>(NodeId folder, Func<string, NodeId, T> create, CancellationToken ct)
        where T : RemoteUaObject
    {
        var children = (await UaUtil.BrowseChildrenAsync(Session, folder, ct).ConfigureAwait(false))
            .Where(c => c.NodeClass == NodeClass.Object)
            .Select(c => create(c.BrowseName.Name, UaUtil.ToNodeId(Session, c.NodeId))).ToList();
        var ok = await InitChildrenAsync(children, ct).ConfigureAwait(false);
        await Task.WhenAll(ok.Select(c => c.ReadUaTypeAsync(ct))).ConfigureAwait(false);
        return ok.ToDictionary(c => c.Name);
    }

    private async Task BrowseSkillSetAsync(NodeId skillSet, CancellationToken ct)
    {
        var skills = await UaUtil.BrowseChildrenAsync(Session, skillSet, ct).ConfigureAwait(false);
        var skillIds = skills.Select(s => UaUtil.ToNodeId(Session, s.NodeId)).ToList();
        var elements = await UaUtil.BrowseChildrenAsync(Session, skillIds, ct).ConfigureAwait(false);
        var created = new List<(string Kind, RemoteSkill Skill)>();
        for (var i = 0; i < skills.Count; i++)
        {
            foreach (var element in elements[i])
            {
                if (element.BrowseName.Name is not ("SkillExecution" or "FeasibilityCheck" or "PreconditionCheck")) continue;
                created.Add((element.BrowseName.Name, new RemoteSkill(skills[i].BrowseName.Name,
                    UaUtil.ToNodeId(Session, element.NodeId), skillIds[i], Server, this, BrowseRules)));
            }
        }
        var ok = await InitChildrenAsync(created.Select(c => c.Skill).ToList(), ct).ConfigureAwait(false);
        await Task.WhenAll(ok.Select(s => s.ReadUaTypeAsync(ct))).ConfigureAwait(false);
        Dictionary<string, RemoteSkill> Of(string kind) =>
            created.Where(c => c.Kind == kind && ok.Contains(c.Skill)).ToDictionary(c => c.Skill.Name, c => c.Skill);
        _skillSet = Of("SkillExecution");
        _feasibilityCheckSet = Of("FeasibilityCheck");
        _preconditionCheckSet = Of("PreconditionCheck");
    }

    private Task OnCurrentStateAsync(DataValue dv)
    {
        var state = StateText(dv.Value);
        if (state == CurrentState) return Task.CompletedTask;
        CurrentState = state;
        return StatusChanged.SendAsync((this, state));
    }

    private Task OnEventAsync(EventFieldList fields)
    {
        var message = fields.EventFields[0].Value is LocalizedText lt ? lt.Text : fields.EventFields[0].Value as string;
        var severity = Convert.ToInt32(fields.EventFields[1].Value ?? 0);
        var notification = NotificationEvent.Parse(message, severity, fields.EventFields[3].Value as string);
        if (notification == null)
        {
            Logger.LogWarning("Dropping event without message on {Path}", Path);
            return Task.CompletedTask;
        }
        return NotificationReceived.SendAsync((this, notification));
    }

    internal static string StateText(object? value) => value switch
    {
        LocalizedText lt => lt.Text ?? "",
        null => "",
        _ => value.ToString() ?? "",
    };
}

public sealed class RemoteComponent : BaseRemoteComponent
{
    internal RemoteComponent(string name, NodeId nodeId, RemoteServer server, RemoteUaObject? parent, IReadOnlyList<BrowseRule>? browseRules)
        : base(name, nodeId, server, parent, browseRules) { }

    /// <summary>Optional for components.</summary>
    public RemoteLock? Lock => LockOrNull;

    /// <summary>
    /// Direct node-id mode: build a component from a known NodeId, e.g. after connecting with empty
    /// <see cref="RemoteServerOptions.BrowseRules"/>. With <paramref name="parent"/> null the path is <c>/name</c>.
    /// </summary>
    public static async Task<RemoteComponent> CreateAsync(RemoteServer server, string name, NodeId nodeId,
        RemoteUaObject? parent = null, IReadOnlyList<BrowseRule>? browseRules = null, CancellationToken cancellationToken = default)
    {
        var component = new RemoteComponent(name, nodeId, server, parent, browseRules);
        await component.ReadUaTypeAsync(cancellationToken).ConfigureAwait(false);
        await component.InitAsync(cancellationToken).ConfigureAwait(false);
        return component;
    }
}

public sealed class RemoteResource : BaseRemoteComponent
{
    internal RemoteResource(string name, NodeId nodeId, RemoteServer server, RemoteUaObject parent, IReadOnlyList<BrowseRule>? browseRules)
        : base(name, nodeId, server, parent, browseRules) { }
}

public sealed class RemoteMachine : BaseRemoteComponent
{
    private List<RemoteUser> _users = [];

    internal RemoteMachine(string name, NodeId nodeId, RemoteServer server, IReadOnlyList<BrowseRule>? browseRules)
        : base(name, nodeId, server, null, browseRules) { }

    /// <summary>Mandatory for machines; throws if the Lock feature was not browsed.</summary>
    public RemoteLock Lock => LockOrNull ?? throw new InvalidOperationException($"{Path} has no browsed Lock (BrowseFeature.Lock)");

    public IReadOnlyList<RemoteUser> Users => _users;

    private protected override async Task BrowseBuildingBlockAsync(string name, NodeId nodeId, BrowseFeature features, CancellationToken ct)
    {
        if (name != "Users" || !features.HasFlag(BrowseFeature.Users)) return;
        var users = (await UaUtil.BrowseChildrenAsync(Session, nodeId, ct).ConfigureAwait(false))
            .Select(u => new RemoteUser(u.BrowseName.Name, UaUtil.ToNodeId(Session, u.NodeId), Server, this)).ToList();
        _users = await InitChildrenAsync(users, ct).ConfigureAwait(false);
    }
}
