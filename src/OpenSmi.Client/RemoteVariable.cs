using Opc.Ua;

namespace OpenSmi.Client;

public sealed record TimestampedValue(object? Value, DateTime Timestamp);

/// <summary>Port of <c>client/remote_variable.py</c>. All values are cached and updated by subscription.</summary>
public sealed class RemoteVariable : RemoteUaObject
{
    private static readonly IReadOnlyDictionary<string, string> NoValidValues = new Dictionary<string, string>();

    internal RemoteVariable(string name, NodeId nodeId, RemoteServer server, RemoteUaObject parent)
        : base(name, nodeId, server, parent) { }

    /// <summary>Cached value, processed: <see cref="ValidValues"/> keys are mapped to their display names.</summary>
    public object? Value => Process(RawValue);
    public Variant RawValue { get; private set; } = Variant.Null;
    /// <summary>ServerTimestamp of the cached value.</summary>
    public DateTime? Timestamp { get; private set; }
    /// <summary>From the per-user access level of the logged-in user.</summary>
    public bool IsWritable { get; private set; }
    public string UaDataType { get; private set; } = "";
    public int UaValueRank { get; private set; }
    /// <summary>For NodeId values: NodeId string → DisplayName of all <c>Utilizes</c> targets, plus "" → "&lt;None&gt;".</summary>
    public IReadOnlyDictionary<string, string> ValidValues { get; private set; } = NoValidValues;
    public EUInformation? Unit { get; private set; }
    public Opc.Ua.Range? Range { get; private set; }
    public event AsyncEventHandler<VariableValueChangedEventArgs>? ValueChanged;
    private readonly Watchers<Variant> _valueWatchers = new();

    /// <summary>Yields <see cref="RawValue"/> first, then every change; a slow reader skips the oldest changes.</summary>
    public IAsyncEnumerable<Variant> WatchValueAsync(CancellationToken cancellationToken = default) =>
        _valueWatchers.WatchAsync(() => RawValue, cancellationToken);

    private protected override async Task InitCoreAsync(CancellationToken ct)
    {
        var r = await UaUtil.ReadAsync(Session, [(NodeId, Opc.Ua.Attributes.DataType), (NodeId, Opc.Ua.Attributes.Value),
            (NodeId, Opc.Ua.Attributes.ValueRank), (NodeId, Opc.Ua.Attributes.UserAccessLevel)], ct).ConfigureAwait(false);
        var dataType = r[0].Value as NodeId;
        UaValueRank = r[2].Value is int rank ? rank : 0;
        IsWritable = r[3].Value is byte level && (level & AccessLevels.CurrentWrite) != 0;

        if (dataType is { NamespaceIndex: 0, IdType: IdType.Numeric })
            UaDataType = DataTypes.GetBrowseName((int)(uint)dataType.Identifier) ?? "";
        if (r[1].Value is NodeId)
        {
            var refs = (await UaUtil.BrowseAsync(Session, [NodeId], ReferenceTypeIds.Utilizes, BrowseDirection.Both, 0, ct)
                .ConfigureAwait(false))[0];
            ValidValues = new Dictionary<string, string> { [""] = "<None>" }
                .Concat(refs.Select(x => System.Collections.Generic.KeyValuePair.Create(UaUtil.ToNodeId(Session, x.NodeId).ToString(), x.DisplayName.Text)))
                .DistinctBy(kv => kv.Key).ToDictionary();
        }
        else if (dataType is { NamespaceIndex: > 0 })
        {
            var name = await UaUtil.ReadAsync(Session, [(dataType, Opc.Ua.Attributes.BrowseName)], ct).ConfigureAwait(false);
            UaDataType = (name[0].Value as QualifiedName)?.Name ?? "";
        }

        var properties = await UaUtil.BrowseChildrenAsync(Session, NodeId, ct).ConfigureAwait(false);
        var units = properties.Child("EngineeringUnits");
        var range = properties.Child("EURange");
        var propertyValues = await UaUtil.ReadAsync(Session,
            new[] { units, range }.Where(p => p != null).Select(p => (UaUtil.ToNodeId(Session, p!.NodeId), Opc.Ua.Attributes.Value)), ct)
            .ConfigureAwait(false);
        foreach (var value in propertyValues.Select(v => (v.Value as ExtensionObject)?.Body))
        {
            if (value is EUInformation eu) Unit = eu;
            if (value is Opc.Ua.Range rg) Range = rg;
        }

        Update(r[1]);
    }

    public async Task<object?> ReadValueAsync(bool updateCache = false, CancellationToken cancellationToken = default)
    {
        var dv = await ReadDataValueAsync(cancellationToken).ConfigureAwait(false);
        if (updateCache) await OnDataChangeAsync(dv).ConfigureAwait(false);
        return Process(dv.WrappedValue);
    }

    public async Task<Variant> ReadRawValueAsync(CancellationToken cancellationToken = default) =>
        (await ReadDataValueAsync(cancellationToken).ConfigureAwait(false)).WrappedValue;

    /// <summary>
    /// Writes <paramref name="value"/> converted to the type of the cached value (e.g. int → Double, string →
    /// NodeId/LocalizedText). A rejected string that is a display name of <see cref="ValidValues"/> is retried as its key.
    /// </summary>
    public async Task WriteValueAsync(object? value, CancellationToken cancellationToken = default)
    {
        var status = await WriteAsync(ToWriteValue(value), cancellationToken).ConfigureAwait(false);
        if (StatusCode.IsGood(status)) return;
        if (status.Code is StatusCodes.BadUserAccessDenied or StatusCodes.BadRequiresLock or StatusCodes.BadOutOfRange)
            throw OpenSmiStatusException.From(status, $"Write of {Path}");
        var key = value is string s ? ValidValues.FirstOrDefault(kv => kv.Value == s).Key : null;
        if (key != null && StatusCode.IsGood(await WriteAsync(ToWriteValue(key), cancellationToken).ConfigureAwait(false))) return;
        throw new OpenSmiInvalidValueException($"{value} is not a valid value for {Path} ({status})");
    }

    internal WriteValue ToWriteValue(object? value)
    {
        if (!IsWritable) throw new OpenSmiInvalidValueException($"{Path} is not writable");
        return new WriteValue { NodeId = NodeId, AttributeId = Opc.Ua.Attributes.Value, Value = new DataValue(ToVariant(value)) };
    }

    private Variant ToVariant(object? value)
    {
        if (value is Variant v) return v;
        var type = RawValue.TypeInfo?.BuiltInType ?? BuiltInType.Null;
        return (type, value) switch
        {
            (BuiltInType.NodeId, string s) => new Variant(s.Length == 0 ? NodeId.Null : NodeId.Parse(s)),
            (BuiltInType.NodeId, null) => new Variant(NodeId.Null),
            (BuiltInType.LocalizedText, string s) => new Variant(new LocalizedText("en-US", s)),
            (BuiltInType.Null, _) or (_, null) => new Variant(value),
            _ => new Variant(TypeInfo.Cast(value, type)),
        };
    }

    private async Task<StatusCode> WriteAsync(WriteValue write, CancellationToken ct) =>
        (await Session.WriteAsync(null, [write], ct).ConfigureAwait(false)).Results[0];

    /// <summary>Raw history (HistoryRead). <paramref name="numValues"/> 0 = unlimited; entries without value are skipped.</summary>
    public async Task<IReadOnlyList<TimestampedValue>> ReadHistoryAsync(DateTime? start = null, DateTime? end = null,
        uint numValues = 0, bool returnBounds = true, CancellationToken cancellationToken = default)
    {
        // DateTime.MinValue encodes as "unspecified", like asyncua's win epoch default.
        var details = new ExtensionObject(new ReadRawModifiedDetails
        {
            StartTime = start ?? DateTime.MinValue, EndTime = end ?? DateTime.MinValue,
            NumValuesPerNode = numValues, ReturnBounds = returnBounds,
        });
        var result = new List<TimestampedValue>();
        byte[]? continuation = null;
        do
        {
            var response = await Session.HistoryReadAsync(null, details, TimestampsToReturn.Both, false,
                [new HistoryReadValueId { NodeId = NodeId, ContinuationPoint = continuation }], cancellationToken).ConfigureAwait(false);
            var r = response.Results[0];
            OpenSmiStatusException.ThrowIfBad(r.StatusCode, $"History read of {Path}");
            if (ExtensionObject.ToEncodeable(r.HistoryData) is HistoryData data)
                result.AddRange(data.DataValues.Where(dv => dv.Value != null).Select(dv => new TimestampedValue(dv.Value, dv.ServerTimestamp)));
            continuation = r.ContinuationPoint is { Length: > 0 } cp ? cp : null;
        } while (continuation != null && (numValues == 0 || result.Count < numValues));
        return result;
    }

    internal object? Process(Variant raw)
    {
        var value = raw.Value;
        if (value == null || value is NodeId n && NodeId.IsNull(n)) return null;
        if (ValidValues.Count > 0 && (value is string || IsInteger(value)))
            return ValidValues.TryGetValue(value.ToString()!, out var display) ? display : $"INVALID ENUM VALUE: {value}";
        return value;
    }

    /// <returns>Whether the cached value changed.</returns>
    private bool Update(DataValue dv)
    {
        Timestamp = dv.ServerTimestamp == DateTime.MinValue ? null : dv.ServerTimestamp;
        if (RawValue.Equals(dv.WrappedValue)) return false;
        RawValue = dv.WrappedValue;
        return true;
    }

    internal Task OnDataChangeAsync(DataValue dv)
    {
        if (!Update(dv)) return Task.CompletedTask;
        _valueWatchers.Publish(RawValue);
        return ValueChanged.RaiseAsync(this, new(RawValue), Logger);
    }

    private async Task<DataValue> ReadDataValueAsync(CancellationToken ct)
    {
        var dv = (await UaUtil.ReadAsync(Session, [(NodeId, Opc.Ua.Attributes.Value)], ct).ConfigureAwait(false))[0];
        OpenSmiStatusException.ThrowIfBad(dv.StatusCode, $"Read of {Path}");
        return dv;
    }

    private static bool IsInteger(object v) => v is sbyte or byte or short or ushort or int or uint or long or ulong;

    public override string ToString() => $"{Name}={Value}{(Unit != null ? " " + Unit.DisplayName.Text : "")}";
}
