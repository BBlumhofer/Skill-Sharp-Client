using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace OpenSmi.Client;

/// <summary>Event from a component's <c>Notification</c> node. Alerts carry an error code.</summary>
public sealed record NotificationEvent(string Message, int Severity, string ErrorCode)
{
    /// <summary>
    /// The ErrorCode of an alert event; only without one, a <c>"&lt;msg&gt;|||&lt;code&gt;"</c> suffix of older
    /// servers is split off. Returns <c>null</c> for events without a message (Python drops them, too).
    /// </summary>
    public static NotificationEvent? Parse(string? message, int severity, string? errorCode)
    {
        if (message == null) return null;
        if (errorCode == null && message.Split("|||", 2) is [var text, var code]) return new(text, severity, code);
        return new(message, severity, errorCode ?? "");
    }
}

/// <summary>Port of <c>BaseRemoteComponent</c>: a server-side BaseMachineryItem.</summary>
public abstract class BaseRemoteComponent : RemoteUaObject
{
    private Dictionary<string, RemoteSkill> _skillSet = [];
    private Dictionary<string, RemoteSkillCheck> _feasibilityCheckSet = [], _preconditionCheckSet = [];
    private Dictionary<string, RemoteMethod> _methodSet = [];
    private Dictionary<string, RemoteComponent> _components = [];
    private Dictionary<string, RemoteResource> _resources = [];

    private protected BaseRemoteComponent(string name, string? segment, NodeId nodeId, RemoteServer server, RemoteUaObject? parent,
        IReadOnlyList<BrowseRule>? browseRules) : base(name, nodeId, server, parent, browseRules, segment)
    {
        Identification = new(server, this);
        Monitoring = new(server, this);
        Attributes = new(server, this);
        ParameterSet = new(server, this);
    }

    /// <summary>Skills by path segment.</summary>
    public IReadOnlyDictionary<string, RemoteSkill> SkillSet => _skillSet;
    /// <summary>Feasibility checks by the segment of their skill; their paths lie below the skill.</summary>
    public IReadOnlyDictionary<string, RemoteSkillCheck> FeasibilityCheckSet => _feasibilityCheckSet;
    /// <summary>Precondition checks by the segment of their skill; their paths lie below the skill.</summary>
    public IReadOnlyDictionary<string, RemoteSkillCheck> PreconditionCheckSet => _preconditionCheckSet;
    public IReadOnlyDictionary<string, RemoteMethod> MethodSet => _methodSet;
    public IReadOnlyDictionary<string, RemoteComponent> Components => _components;
    public IReadOnlyDictionary<string, RemoteResource> Resources => _resources;
    public Identification Identification { get; }
    public Monitoring Monitoring { get; }
    public Attributes Attributes { get; }
    public ParameterSet ParameterSet { get; }
    /// <summary>
    /// Cached state; <c>null</c> without a CurrentState node (e.g. resources), while the server reported none yet, or
    /// with a bad status (see <see cref="CurrentStateSample"/>). Empty or unknown values keep the last valid state.
    /// </summary>
    public MachineryItemState? CurrentState => CurrentStateSample?.Value;
    /// <summary>Cached state with status and timestamps; <c>null</c> without a CurrentState node.</summary>
    public Sample<MachineryItemState?>? CurrentStateSample => _currentStateNode == null ? null : _currentStateSample;
    private Sample<MachineryItemState?> _currentStateSample = SampleUpdates.WaitingForInitialData<MachineryItemState?>(null);
    public event AsyncEventHandler<ComponentStatusChangedEventArgs>? StatusChanged;
    public event AsyncEventHandler<NotificationReceivedEventArgs>? NotificationReceived;

    internal RemoteLock? LockOrNull { get; private set; }
    private NodeId? _currentStateNode;

    /// <summary>Reads MachineryItemState/CurrentState; <c>null</c> without the node or for an empty value.</summary>
    /// <exception cref="OpenSmiUnknownStateException">The server reports a state that is no <see cref="MachineryItemState"/>.</exception>
    public async Task<MachineryItemState?> ReadCurrentStateAsync(CancellationToken cancellationToken = default)
    {
        if (_currentStateNode == null) return null;
        var dv = (await UaUtil.ReadAsync(Session, [(_currentStateNode, Opc.Ua.Attributes.Value)], cancellationToken).ConfigureAwait(false))[0];
        OpenSmiStatusException.ThrowIfBad(dv.StatusCode, $"Read of {Path} current state");
        return SampleUpdates.ParseItemState(dv.Value);
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
                    _components = await BrowseFolderAsync(id, (n, s, i) => new RemoteComponent(n, s, i, Server, this, BrowseRules), ct).ConfigureAwait(false),
                "Resources" when features.HasFlag(BrowseFeature.Resources) => async () =>
                    _resources = await BrowseFolderAsync(id, (n, s, i) => new RemoteResource(n, s, i, Server, this, BrowseRules), ct).ConfigureAwait(false),
                "MethodSet" when features.HasFlag(BrowseFeature.MethodSet) => async () =>
                    _methodSet = await BrowseFolderAsync(id, (n, s, i) => new RemoteMethod(n, s, i, Server, this, BrowseRules), ct).ConfigureAwait(false),
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

    private async Task BrowseBuildingBlocksAsync(NodeId nodeId, BrowseFeature features, CancellationToken ct)
    {
        foreach (var child in await UaUtil.BrowseChildrenAsync(Session, nodeId, ct).ConfigureAwait(false))
        {
            var id = UaUtil.ToNodeId(Session, child.NodeId);
            if (child.BrowseName.Name != "MachineryItemState") continue; // Users belong to the server

            var state = (await UaUtil.BrowseChildrenAsync(Session, id, ct).ConfigureAwait(false)).Child("CurrentState");
            if (state == null) continue;
            var stateNode = UaUtil.ToNodeId(Session, state.NodeId);
            // read before subscribing, so the state is known when ConnectAsync returns (like skills)
            await OnCurrentStateAsync((await UaUtil.ReadAsync(Session, [(stateNode, Opc.Ua.Attributes.Value)], ct).ConfigureAwait(false))[0])
                .ConfigureAwait(false);
            _currentStateNode = stateNode;
            await Server.Subscriptions.SubscribeDataChangeAsync([(stateNode, OnCurrentStateAsync)], ct).ConfigureAwait(false);
        }
    }

    private async Task InitLockAsync(NodeId nodeId, CancellationToken ct)
    {
        var remoteLock = new RemoteLock(nodeId, Server, this);
        await remoteLock.InitAsync(ct).ConfigureAwait(false);
        await remoteLock.ReadUaTypeAsync(ct).ConfigureAwait(false);
        LockOrNull = remoteLock;
    }

    /// <summary>Objects of a folder (Components, Resources, MethodSet), keyed by their unique segment.</summary>
    private async Task<Dictionary<string, T>> BrowseFolderAsync<T>(NodeId folder, Func<string, string, NodeId, T> create, CancellationToken ct)
        where T : RemoteUaObject
    {
        var refs = await UaUtil.BrowseChildrenAsync(Session, folder, ct).ConfigureAwait(false);
        var segments = UniqueSegments(refs);
        var children = refs.Where(c => c.NodeClass == NodeClass.Object)
            .Select(c => UaUtil.ToNodeId(Session, c.NodeId)).Distinct()
            .Select(id => create(refs.First(r => UaUtil.ToNodeId(Session, r.NodeId) == id).BrowseName.Name ?? "", segments[id], id)).ToList();
        var ok = await InitChildrenAsync(children, ct).ConfigureAwait(false);
        await Task.WhenAll(ok.Select(c => c.ReadUaTypeAsync(ct))).ConfigureAwait(false);
        return ok.ToDictionary(c => c.Segment);
    }

    /// <summary>
    /// A skill is the <c>SkillExecution</c> of a skill object; its checks lie below it (<c>.../SkillSet/Pick/FeasibilityCheck</c>).
    /// A skill object without SkillExecution is skipped.
    /// </summary>
    private async Task BrowseSkillSetAsync(NodeId skillSet, CancellationToken ct)
    {
        var skills = await UaUtil.BrowseChildrenAsync(Session, skillSet, ct).ConfigureAwait(false);
        var segments = UniqueSegments(skills);
        var skillIds = skills.Select(s => UaUtil.ToNodeId(Session, s.NodeId)).ToList();
        var elements = await UaUtil.BrowseChildrenAsync(Session, skillIds, ct).ConfigureAwait(false);
        var created = new List<(string Kind, RemoteSkill Skill)>();
        for (var i = 0; i < skills.Count; i++)
        {
            NodeId? Element(string name) => elements[i].Child(name) is { } c ? UaUtil.ToNodeId(Session, c.NodeId) : null;
            if (Element("SkillExecution") is not { } execution)
            {
                Logger.LogWarning("Skill {Name} without SkillExecution, skipped ({NodeId})", skills[i].BrowseName.Name, skillIds[i]);
                continue;
            }
            // the skill first: its checks' paths lie below it, and objects register top-down
            var skill = new RemoteSkill(skills[i].BrowseName.Name ?? "", segments[skillIds[i]], execution, skillIds[i], Server, this, BrowseRules);
            created.Add(("SkillExecution", skill));
            foreach (var kind in new[] { "FeasibilityCheck", "PreconditionCheck" })
                if (Element(kind) is { } check)
                    created.Add((kind, new RemoteSkillCheck(skill, kind, check, skillIds[i], Server, this, BrowseRules)));
        }
        var ok = await InitChildrenAsync(created.Select(c => c.Skill).ToList(), ct).ConfigureAwait(false);
        await Task.WhenAll(ok.Select(s => s.ReadUaTypeAsync(ct))).ConfigureAwait(false);
        IEnumerable<RemoteSkill> Of(string kind) => created.Where(c => c.Kind == kind && ok.Contains(c.Skill)).Select(c => c.Skill);
        _skillSet = Of("SkillExecution").ToDictionary(s => s.Segment);
        _feasibilityCheckSet = Of("FeasibilityCheck").Cast<RemoteSkillCheck>().ToDictionary(c => c.Skill.Segment);
        _preconditionCheckSet = Of("PreconditionCheck").Cast<RemoteSkillCheck>().ToDictionary(c => c.Skill.Segment);
    }

    private Task OnCurrentStateAsync(DataValue dv)
    {
        if (SampleUpdates.ComponentState(_currentStateSample, dv) is not var (sample, changed))
        {
            Logger.LogWarning("Ignoring empty or unknown machinery item state {Value} of {Path}", dv.Value, Path);
            return Task.CompletedTask;
        }
        _currentStateSample = sample;
        return changed ? StatusChanged.RaiseAsync(this, new(sample), Logger) : Task.CompletedTask;
    }

    private Task OnEventAsync(EventFieldList fields)
    {
        var message = fields.EventFields[0].Value is LocalizedText lt ? lt.Text : fields.EventFields[0].Value as string;
        var severity = Convert.ToInt32(fields.EventFields[1].Value ?? 0);
        var notification = NotificationEvent.Parse(message, severity, fields.EventFields[2].Value as string);
        if (notification == null)
        {
            Logger.LogWarning("Dropping event without message on {Path}", Path);
            return Task.CompletedTask;
        }
        return NotificationReceived.RaiseAsync(this, new(notification), Logger);
    }
}

public sealed class RemoteComponent : BaseRemoteComponent
{
    internal RemoteComponent(string name, string? segment, NodeId nodeId, RemoteServer server, RemoteUaObject? parent, IReadOnlyList<BrowseRule>? browseRules)
        : base(name, segment, nodeId, server, parent, browseRules) { }

    private protected override string Folder => "Components";

    /// <summary>Optional for components.</summary>
    public RemoteLock? Lock => LockOrNull;

    /// <summary>
    /// Direct node-id mode: build a component from a known NodeId, e.g. after connecting with empty
    /// <see cref="RemoteServerOptions.BrowseRules"/>. With <paramref name="parent"/> null the path is <c>/Components/name</c>.
    /// </summary>
    public static async Task<RemoteComponent> CreateAsync(RemoteServer server, string name, NodeId nodeId,
        RemoteUaObject? parent = null, IReadOnlyList<BrowseRule>? browseRules = null, CancellationToken cancellationToken = default)
    {
        var component = new RemoteComponent(name, null, nodeId, server, parent, browseRules);
        await component.ReadUaTypeAsync(cancellationToken).ConfigureAwait(false);
        await component.InitAsync(cancellationToken).ConfigureAwait(false);
        return component;
    }
}

public sealed class RemoteResource : BaseRemoteComponent
{
    internal RemoteResource(string name, string segment, NodeId nodeId, RemoteServer server, RemoteUaObject parent, IReadOnlyList<BrowseRule>? browseRules)
        : base(name, segment, nodeId, server, parent, browseRules) { }

    private protected override string Folder => "Resources";
}

public sealed class RemoteMachine : BaseRemoteComponent
{
    internal RemoteMachine(string name, string segment, NodeId nodeId, RemoteServer server, IReadOnlyList<BrowseRule>? browseRules)
        : base(name, segment, nodeId, server, null, browseRules) { }

    /// <summary>Mandatory for machines; throws if the Lock feature was not browsed.</summary>
    public RemoteLock Lock => LockOrNull ?? throw new InvalidOperationException($"{Path} has no browsed Lock (BrowseFeature.Lock)");
}
