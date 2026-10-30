namespace OpenSmi.Client;

// Port of client/dto.py: snapshots of cached values, no I/O. Plain records, serializable with System.Text.Json.

public record RemoteUaObjectDto
{
    public required string Name { get; init; }
    public required string Path { get; init; }
    public required string UaNodeId { get; init; }
    public required string UaType { get; init; }
}

public sealed record RangeDto(double Low, double High);

public sealed record RemoteVariableDto : RemoteUaObjectDto
{
    /// <summary>Primitives as they are; other OPC UA types (NodeId, LocalizedText …) as string.</summary>
    public object? Value { get; init; }
    public DateTime? Timestamp { get; init; }
    public bool IsWritable { get; init; }
    public required IReadOnlyDictionary<string, string> ValidValues { get; init; }
    /// <summary>Unit symbol (EUInformation.DisplayName).</summary>
    public string? Unit { get; init; }
    public RangeDto? Range { get; init; }
    public required string UaDataType { get; init; }
    public int UaValueRank { get; init; }
}

public record RemoteCallableDto : RemoteUaObjectDto
{
    public int? MinAccessLevel { get; init; }
    public required IReadOnlyList<RemoteVariableDto> ParameterSet { get; init; }
    public required IReadOnlyList<RemoteVariableDto> Monitoring { get; init; }
    public required IReadOnlyList<RemoteVariableDto> FinalResultData { get; init; }
    public required IReadOnlyList<string> UaDependsOnNodeIds { get; init; }
    public required IReadOnlyList<string> UaDependencyOfNodeIds { get; init; }
}

public sealed record RemoteSkillDto : RemoteCallableDto
{
    public SkillState CurrentState { get; init; }
    public bool IsSuspendable { get; init; }
    public bool IsFinite { get; init; }
}

public sealed record RemoteLockDto : RemoteUaObjectDto
{
    public bool IsLocked { get; init; }
    public required string LockingUser { get; init; }
    public required string LockingClient { get; init; }
    public DateTime? LockedSince { get; init; }
    public int? RemainingLockTimeMs { get; init; }
    public int MaxInactiveLockTimeMs { get; init; }
}

public sealed record RemoteUserDto : RemoteUaObjectDto
{
    public bool AllowMultiple { get; init; }
    /// <summary>Python calls this <c>current_access_level</c>, but the server fills it with the lock priority.</summary>
    public int? LockPriority { get; init; }
    public int MaxAccessLevel { get; init; }
    public bool IsPresent { get; init; }
}

public sealed record RemoteResourceDto : RemoteUaObjectDto
{
    public required IReadOnlyList<RemoteVariableDto> Attributes { get; init; }
    public required IReadOnlyList<RemoteVariableDto> Identification { get; init; }
    public required IReadOnlyList<RemoteVariableDto> Monitoring { get; init; }
}

public record RemoteComponentDto : RemoteUaObjectDto
{
    public required string CurrentState { get; init; }
    public RemoteLockDto? Lock { get; init; }
    public required IReadOnlyList<RemoteVariableDto> Attributes { get; init; }
    public required IReadOnlyList<RemoteVariableDto> Identification { get; init; }
    public required IReadOnlyList<RemoteVariableDto> Monitoring { get; init; }
    public required IReadOnlyList<RemoteVariableDto> ParameterSet { get; init; }
    public required IReadOnlyList<RemoteSkillDto> SkillSet { get; init; }
    public required IReadOnlyList<RemoteSkillDto> FeasibilityCheckSet { get; init; }
    public required IReadOnlyList<RemoteSkillDto> PreconditionCheckSet { get; init; }
    public required IReadOnlyList<RemoteCallableDto> MethodSet { get; init; }
    public required IReadOnlyList<RemoteComponentDto> Components { get; init; }
    public required IReadOnlyList<RemoteResourceDto> Resources { get; init; }
}

public sealed record RemoteMachineDto : RemoteComponentDto
{
    [System.Diagnostics.CodeAnalysis.SetsRequiredMembers]
    internal RemoteMachineDto(RemoteComponentDto component, IReadOnlyList<RemoteUserDto> users) : base(component) => Users = users;

    public IReadOnlyList<RemoteUserDto> Users { get; init; }
}

public static class DtoExtensions
{
    public static RemoteVariableDto ToDto(this RemoteVariable v) => new()
    {
        Name = v.Name, Path = v.Path, UaNodeId = v.NodeId.ToString(), UaType = v.UaType,
        Value = v.Value is null or string or bool or sbyte or byte or short or ushort or int or uint or long or ulong
            or float or double or DateTime ? v.Value : v.Value.ToString(),
        Timestamp = v.Timestamp, IsWritable = v.IsWritable, ValidValues = v.ValidValues,
        Unit = v.Unit?.DisplayName.Text, Range = v.Range is { } r ? new(r.Low, r.High) : null,
        UaDataType = v.UaDataType, UaValueRank = v.UaValueRank,
    };

    /// <summary>Empty when the container was not browsed.</summary>
    public static IReadOnlyList<RemoteVariableDto> ToDto(this RemoteVariableContainer c) => [.. c.Values.Select(ToDto)];

    public static RemoteCallableDto ToDto(this RemoteMethod m) => new()
    {
        Name = m.Name, Path = m.Path, UaNodeId = m.NodeId.ToString(), UaType = m.UaType,
        MinAccessLevel = m.MinAccessLevel, ParameterSet = m.ParameterSet.ToDto(), Monitoring = m.Monitoring.ToDto(),
        FinalResultData = m.FinalResultData.ToDto(),
        UaDependsOnNodeIds = [.. m.DependsOn.Select(n => n.ToString())],
        UaDependencyOfNodeIds = [.. m.DependencyOf.Select(n => n.ToString())],
    };

    public static RemoteSkillDto ToDto(this RemoteSkill s) => new()
    {
        Name = s.Name, Path = s.Path, UaNodeId = s.NodeId.ToString(), UaType = s.UaType,
        MinAccessLevel = s.MinAccessLevel, ParameterSet = s.ParameterSet.ToDto(), Monitoring = s.Monitoring.ToDto(),
        FinalResultData = s.FinalResultData.ToDto(),
        UaDependsOnNodeIds = [.. s.DependsOn.Select(n => n.ToString())],
        UaDependencyOfNodeIds = [.. s.DependencyOf.Select(n => n.ToString())],
        CurrentState = s.CurrentState, IsSuspendable = s.IsSuspendable, IsFinite = s.IsFinite,
    };

    public static RemoteLockDto ToDto(this RemoteLock l) => new()
    {
        Name = l.Name, Path = l.Path, UaNodeId = l.NodeId.ToString(), UaType = l.UaType,
        IsLocked = l.IsLocked, LockingUser = l.LockingUser, LockingClient = l.LockingClient, LockedSince = l.LockedSince,
        RemainingLockTimeMs = l.RemainingLockTimeMs, MaxInactiveLockTimeMs = l.MaxInactiveLockTimeMs,
    };

    public static RemoteUserDto ToDto(this RemoteUser u) => new()
    {
        Name = u.Name, Path = u.Path, UaNodeId = u.NodeId.ToString(), UaType = u.UaType,
        AllowMultiple = u.Data.AllowMultiple, LockPriority = u.LockPriority, MaxAccessLevel = u.Data.MaxAccessLevel,
        IsPresent = u.Data.IsPresent,
    };

    public static RemoteResourceDto ToDto(this RemoteResource r) => new()
    {
        Name = r.Name, Path = r.Path, UaNodeId = r.NodeId.ToString(), UaType = r.UaType,
        Attributes = r.Attributes.ToDto(), Identification = r.Identification.ToDto(), Monitoring = r.Monitoring.ToDto(),
    };

    public static RemoteComponentDto ToDto(this RemoteComponent c) => ComponentDto(c);

    /// <summary><c>Lock</c> is null when it was not browsed (Python raises an AssertionError instead).</summary>
    public static RemoteMachineDto ToDto(this RemoteMachine m) => new(ComponentDto(m), [.. m.Users.Select(ToDto)]);

    private static RemoteComponentDto ComponentDto(BaseRemoteComponent c) => new()
    {
        Name = c.Name, Path = c.Path, UaNodeId = c.NodeId.ToString(), UaType = c.UaType, CurrentState = c.CurrentState,
        Lock = c.LockOrNull?.ToDto(),
        Attributes = c.Attributes.ToDto(), Identification = c.Identification.ToDto(), Monitoring = c.Monitoring.ToDto(),
        ParameterSet = c.ParameterSet.ToDto(),
        SkillSet = [.. c.SkillSet.Values.Select(ToDto)],
        FeasibilityCheckSet = [.. c.FeasibilityCheckSet.Values.Select(ToDto)],
        PreconditionCheckSet = [.. c.PreconditionCheckSet.Values.Select(ToDto)],
        MethodSet = [.. c.MethodSet.Values.Select(ToDto)],
        Components = [.. c.Components.Values.Select(ToDto)],
        Resources = [.. c.Resources.Values.Select(ToDto)],
    };
}
