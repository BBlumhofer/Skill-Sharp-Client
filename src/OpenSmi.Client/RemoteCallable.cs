using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace OpenSmi.Client;

/// <summary>Port of <c>client/remote_callable.py</c>: base of skills and methods.</summary>
public abstract class RemoteCallable : RemoteUaObject
{
    private readonly List<NodeId> _dependsOn = [], _dependencyOf = [];

    private protected RemoteCallable(string name, string segment, NodeId nodeId, RemoteServer server, BaseRemoteComponent parent,
        IReadOnlyList<BrowseRule>? browseRules) : base(name, nodeId, server, parent, browseRules, segment)
    {
        ParameterSet = new(server, this);
        Monitoring = new(server, this);
        FinalResultData = new(server, this);
    }

    public ParameterSet ParameterSet { get; }
    public Monitoring Monitoring { get; }
    public FinalResultData FinalResultData { get; }
    /// <summary><c>null</c> when the server does not provide it.</summary>
    public int? MinAccessLevel { get; private set; }
    public IReadOnlyList<NodeId> DependsOn => _dependsOn;
    public IReadOnlyList<NodeId> DependencyOf => _dependencyOf;

    private protected override async Task InitCoreAsync(CancellationToken ct)
    {
        var features = OpenSmi.Client.BrowseRules.Evaluate(BrowseRules, Path);
        var work = new List<(string, Func<Task>)>();
        foreach (var child in await UaUtil.BrowseChildrenAsync(Session, NodeId, ct).ConfigureAwait(false))
        {
            var id = UaUtil.ToNodeId(Session, child.NodeId);
            var name = child.BrowseName.Name;
            Func<Task>? run = name switch
            {
                "MinAccessLevel" => async () => MinAccessLevel = Convert.ToInt32(
                    (await UaUtil.ReadAsync(Session, [(id, Opc.Ua.Attributes.Value)], ct).ConfigureAwait(false))[0].Value),
                "Monitoring" => features.HasFlag(BrowseFeature.Monitoring) ? () => Monitoring.InitAsync(id, ct) : null,
                "ParameterSet" => features.HasFlag(BrowseFeature.ParameterSet) ? () => ParameterSet.InitAsync(id, ct) : null,
                "FinalResultData" => features.HasFlag(BrowseFeature.FinalResultData) ? () => FinalResultData.InitAsync(id, ct) : null,
                "Requirements" => () => BrowseDependenciesAsync(id, ct),
                _ => HandleChild(name, id, ct),
            };
            if (run != null) work.Add(($"{Path}/{name}", run));
        }
        if (RequirementsOwner is { } owner)
            work.Add(($"{Path}/Requirements", () => BrowseOwnerRequirementsAsync(owner, ct)));
        await GatherAsync(work).ConfigureAwait(false);
        await Server.Subscriptions.SubscribeDataChangeAsync(
            new RemoteVariableContainer[] { Monitoring, FinalResultData, ParameterSet }.SelectMany(c => c.SubscriptionItems),
            ct).ConfigureAwait(false);
    }

    /// <summary>A further node whose <c>Requirements</c> belong to this callable (the skill object of a skill).</summary>
    private protected virtual NodeId? RequirementsOwner => null;

    private async Task BrowseOwnerRequirementsAsync(NodeId owner, CancellationToken ct)
    {
        if ((await UaUtil.BrowseChildrenAsync(Session, owner, ct).ConfigureAwait(false)).Child("Requirements") is { } requirements)
            await BrowseDependenciesAsync(UaUtil.ToNodeId(Session, requirements.NodeId), ct).ConfigureAwait(false);
    }

    /// <summary>Handles the type-specific children (Call, StateMachine); null if nothing to do.</summary>
    private protected abstract Func<Task>? HandleChild(string name, NodeId nodeId, CancellationToken ct);

    private async Task BrowseDependenciesAsync(NodeId requirements, CancellationToken ct)
    {
        var refs = (await UaUtil.BrowseAsync(Session, [requirements], ReferenceTypeIds.Requires, BrowseDirection.Both, 0, ct)
            .ConfigureAwait(false))[0];
        lock (_dependsOn)
            foreach (var r in refs)
            {
                var list = r.IsForward ? _dependsOn : _dependencyOf;
                var id = UaUtil.ToNodeId(Session, r.NodeId);
                if (!list.Contains(id)) list.Add(id);
            }
    }
}

/// <summary>Port of <c>client/remote_method.py</c>.</summary>
public sealed class RemoteMethod : RemoteCallable
{
    private NodeId? _call;

    internal RemoteMethod(string name, string segment, NodeId nodeId, RemoteServer server, BaseRemoteComponent parent, IReadOnlyList<BrowseRule>? browseRules)
        : base(name, segment, nodeId, server, parent, browseRules) { }

    private protected override string Folder => "MethodSet";

    /// <summary>
    /// Calls <c>Call</c> without input arguments (write <see cref="RemoteCallable.ParameterSet"/> first) and returns
    /// <see cref="RemoteCallable.FinalResultData"/>.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, object?>> CallAsync(CancellationToken cancellationToken = default)
    {
        await UaUtil.CallAsync(Session, NodeId, _call ?? throw new OpenSmiProtocolException($"{Path} has no Call method"), Path, cancellationToken)
            .ConfigureAwait(false);
        return await FinalResultData.ReadAllAsync(cancellationToken).ConfigureAwait(false);
    }

    private protected override Func<Task>? HandleChild(string name, NodeId nodeId, CancellationToken ct)
    {
        if (name == "Call") _call = nodeId;
        return null;
    }
}

/// <summary>
/// Port of <c>client/remote_skill.py</c>. <see cref="RemoteUaObject.NodeId"/> is the <c>SkillExecution</c>
/// node; <see cref="RemoteUaObject.Name"/> is the skill object's name.
/// </summary>
public class RemoteSkill : RemoteCallable
{
    private readonly NodeId _skillObject;
    private readonly object _stateGate = new(); // not System.Threading.Lock: net8.0 has none
    private TaskCompletionSource _stateChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _haltCount;
    private NodeId? _stateMachine, _currentState, _start, _halt, _reset, _suspend;

    internal RemoteSkill(string name, string segment, NodeId nodeId, NodeId skillObject, RemoteServer server, BaseRemoteComponent parent,
        IReadOnlyList<BrowseRule>? browseRules) : base(name, segment, nodeId, server, parent, browseRules)
    {
        _skillObject = skillObject;
    }

    private protected override string? Folder => "SkillSet";

    /// <summary>
    /// opensmi-server 0.3 puts <c>Requirements</c> below the skill object, older servers below SkillExecution;
    /// both are read (Python reads only SkillExecution).
    /// </summary>
    private protected override NodeId? RequirementsOwner => _skillObject;

    /// <summary>
    /// Cached state, updated by subscription. Halted (the safest assumption) while the server reports no value, e.g.
    /// with a bad status (see <see cref="CurrentStateSample"/>).
    /// </summary>
    public SkillState CurrentState => CurrentStateSample.Value;
    /// <summary>Cached state with status and timestamps; BadWaitingForInitialData until the server reported one.</summary>
    public Sample<SkillState> CurrentStateSample { get; private set; } = SampleUpdates.WaitingForInitialData(SkillState.Halted);
    public bool IsSuspendable => _suspend != null;
    /// <summary>From the type of the skill object (FiniteSkillType vs. ContinuousSkillType).</summary>
    public bool IsFinite => UaType.StartsWith("finite", StringComparison.OrdinalIgnoreCase);
    public event AsyncEventHandler<SkillStateChangedEventArgs>? StateChanged;
    private readonly Watchers<SkillState> _stateWatchers = new();

    /// <summary>Yields <see cref="CurrentState"/> first, then every change; a slow reader skips the oldest changes.</summary>
    public IAsyncEnumerable<SkillState> WatchStateAsync(CancellationToken cancellationToken = default) =>
        _stateWatchers.WatchAsync(() => CurrentState, cancellationToken);

    public Task StartAsync(StartWait wait = StartWait.None, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        if (wait == StartWait.Completion && !IsFinite)
            throw new InvalidOperationException($"{Path} is continuous and never completes");
        SkillState[] targets = wait == StartWait.Completion ? [SkillState.Completed] : [SkillState.Running, SkillState.Completed];
        return TriggerAsync(_start, wait != StartWait.None, targets, timeout, cancellationToken);
    }

    public Task ResetAsync(bool wait = false, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        TriggerAsync(_reset, wait, [SkillState.Ready], timeout, cancellationToken);

    public Task HaltAsync(bool wait = false, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        TriggerAsync(_halt, wait, [SkillState.Halted], timeout, cancellationToken);

    public Task SuspendAsync(bool wait = false, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        _suspend == null
            ? throw new SkillNotSuspendableException($"{Path} is not suspendable")
            : TriggerAsync(_suspend, wait, [SkillState.Suspended], timeout, cancellationToken);

    /// <summary>
    /// Waits until the cached state is <paramref name="target"/>. Only the latest state is observed, so prefer
    /// stable states. Throws <see cref="SkillHaltedException"/> if the skill halts meanwhile (unless the target is Halted).
    /// </summary>
    public Task WaitForStateAsync(SkillState target, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        WaitForStatesAsync([target], Volatile.Read(ref _haltCount), timeout, cancellationToken);

    public async Task<SkillState> ReadCurrentStateAsync(CancellationToken cancellationToken = default)
    {
        var dv = (await UaUtil.ReadAsync(Session, [(_currentState!, Opc.Ua.Attributes.Value)], cancellationToken).ConfigureAwait(false))[0];
        OpenSmiStatusException.ThrowIfBad(dv.StatusCode, $"Read of {Path} state");
        return dv.Value == null ? SkillState.Halted : ParseState(SampleUpdates.StateText(dv.Value));
    }

    public override async Task<string> ReadUaTypeAsync(CancellationToken cancellationToken = default)
    {
        return UaType = await UaUtil.ReadTypeNameAsync(Session, _skillObject, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Empty text means Halted ("assume the worst"); unknown text is a protocol error.</summary>
    internal static SkillState ParseState(string text) =>
        text.Length == 0 ? SkillState.Halted
        : Enum.TryParse<SkillState>(text, ignoreCase: true, out var s) && Enum.IsDefined(s) && !int.TryParse(text, out _) ? s
        : throw new OpenSmiUnknownStateException($"Unknown skill state '{text}'");

    private protected override Func<Task>? HandleChild(string name, NodeId nodeId, CancellationToken ct) =>
        name == "StateMachine" ? () => BrowseStateMachineAsync(nodeId, ct) : null;

    private async Task BrowseStateMachineAsync(NodeId stateMachine, CancellationToken ct)
    {
        var children = await UaUtil.BrowseChildrenAsync(Session, stateMachine, ct).ConfigureAwait(false);
        NodeId Required(string name) => children.Child(name) is { } c ? UaUtil.ToNodeId(Session, c.NodeId)
            : throw new OpenSmiProtocolException($"{Path}: StateMachine has no '{name}'");
        _stateMachine = stateMachine;
        _currentState = Required("CurrentState");
        _start = Required("Start");
        _halt = Required("Halt");
        _reset = Required("Reset");
        _suspend = children.Child("Suspend") is { } s ? UaUtil.ToNodeId(Session, s.NodeId) : null;
        // The state is read before subscribing, so it is known when ConnectAsync returns (Python waits for the
        // first notification); the subscription's initial value then only confirms it.
        var initial = (await UaUtil.ReadAsync(Session, [(_currentState, Opc.Ua.Attributes.Value)], ct).ConfigureAwait(false))[0];
        await OnStateAsync(initial).ConfigureAwait(false);
        await Server.Subscriptions.SubscribeDataChangeAsync([(_currentState, OnStateAsync)], ct).ConfigureAwait(false);
    }

    private async Task TriggerAsync(NodeId? method, bool wait, SkillState[] targets, TimeSpan? timeout, CancellationToken ct)
    {
        var haltCount = Volatile.Read(ref _haltCount); // before the call, so a halt during the call is noticed
        await UaUtil.CallAsync(Session, _stateMachine!, method!, Path, ct).ConfigureAwait(false);
        if (wait) await WaitForStatesAsync(targets, haltCount, timeout, ct).ConfigureAwait(false);
    }

    private async Task WaitForStatesAsync(SkillState[] targets, int haltCount, TimeSpan? timeout, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (timeout is { } t) cts.CancelAfter(t);
        var expectHalt = targets.Contains(SkillState.Halted);
        while (true)
        {
            Task changed;
            lock (_stateGate)
            {
                if (targets.Contains(CurrentState)) return;
                if (!expectHalt && _haltCount > haltCount) throw new SkillHaltedException($"Skill '{Name}' halted unexpectedly");
                changed = _stateChanged.Task;
            }
            try { await changed.WaitAsync(cts.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                throw new TimeoutException($"{Path} did not reach {string.Join("|", targets)} within {timeout}");
            }
        }
    }

    private Task OnStateAsync(DataValue dv)
    {
        Sample<SkillState> sample;
        lock (_stateGate)
        {
            if (SampleUpdates.SkillState(CurrentStateSample, dv) is not var (s, changed, stateChanged))
            {
                Logger.LogWarning("Ignoring unknown state {Value} of {Path}", dv.Value, Path);
                return Task.CompletedTask;
            }
            CurrentStateSample = sample = s;
            if (!changed) return Task.CompletedTask; // e.g. only a new timestamp
            if (stateChanged)
            {
                if (s.Value == SkillState.Halted) _haltCount++;
                _stateChanged.SetResult();
                _stateChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _stateWatchers.Publish(s.Value);
            }
        }
        return StateChanged.RaiseAsync(this, new(sample), Logger);
    }
}

/// <summary>
/// A feasibility or precondition check of a skill: a skill-like object of the component whose path lies below its
/// skill, e.g. <c>/M1/SkillSet/Pick/FeasibilityCheck</c>. Name and segment are the check's browse name.
/// </summary>
public sealed class RemoteSkillCheck(RemoteSkill skill, string name, NodeId nodeId, NodeId skillObject, RemoteServer server,
    BaseRemoteComponent parent, IReadOnlyList<BrowseRule>? browseRules)
    : RemoteSkill(name, name, nodeId, skillObject, server, parent, browseRules)
{
    private readonly RemoteSkill _skill = skill; // initialized before the base constructor registers the path

    /// <summary>The skill this check belongs to.</summary>
    public RemoteSkill Skill => _skill;

    private protected override ObjectPath BasePath => _skill.Path;

    /// <summary>The skill object's Requirements belong to the skill, not to its checks.</summary>
    private protected override NodeId? RequirementsOwner => null;
}
