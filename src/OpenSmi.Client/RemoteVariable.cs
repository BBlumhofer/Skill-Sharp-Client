using System.Globalization;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace OpenSmi.Client;

/// <summary>Port of <c>client/remote_variable.py</c>. All values are cached and updated by subscription.</summary>
public sealed class RemoteVariable : RemoteUaObject
{
    private static readonly IReadOnlyDictionary<string, string> NoValidValues = new Dictionary<string, string>();

    internal RemoteVariable(string name, string segment, NodeId nodeId, RemoteServer server, RemoteUaObject parent)
        : base(name, nodeId, server, parent, segment: segment) { }

    /// <summary>
    /// Cached sample: processed value (<see cref="ValidValues"/> keys mapped to their display names), status and
    /// timestamps. BadWaitingForInitialData until the first value was read or received.
    /// </summary>
    public Sample<object?> Sample { get; private set; } = SampleUpdates.WaitingForInitialData<object?>(null);
    /// <summary>Cached value, processed: <see cref="ValidValues"/> keys are mapped to their display names.</summary>
    public object? Value => Sample.Value;
    public Variant RawValue => Sample.Variant ?? Variant.Null;
    /// <summary>Timestamp of the cached sample: source timestamp, falling back to the server timestamp.</summary>
    public DateTime? Timestamp => Sample.Timestamp;
    /// <summary>From the per-user access level of the logged-in user.</summary>
    public bool IsWritable { get; private set; }
    public string UaDataType { get; private set; } = "";
    /// <summary>-1 scalar (the OPC UA default, also if it cannot be read), 0 one or more dimensions, &gt;= 1 exact dimensions.</summary>
    public int UaValueRank { get; private set; } = ValueRanks.Scalar;
    /// <summary>For NodeId values: NodeId string → DisplayName of all <c>Utilizes</c> targets, plus "" → "&lt;None&gt;".
    /// For custom enum DataTypes: value ("0") → member name.</summary>
    public IReadOnlyDictionary<string, string> ValidValues { get; private set; } = NoValidValues;
    public EUInformation? Unit { get; private set; }
    public Opc.Ua.Range? Range { get; private set; }
    public event AsyncEventHandler<VariableValueChangedEventArgs>? ValueChanged;
    private readonly Watchers<Sample<object?>> _valueWatchers = new();

    /// <summary>Yields <see cref="Sample"/> first, then every change; a slow reader skips the oldest changes.</summary>
    public IAsyncEnumerable<Sample<object?>> WatchValueAsync(CancellationToken cancellationToken = default) =>
        _valueWatchers.WatchAsync(() => Sample, cancellationToken);

    private protected override async Task InitCoreAsync(CancellationToken ct)
    {
        var r = await UaUtil.ReadAsync(Session, [(NodeId, Opc.Ua.Attributes.DataType), (NodeId, Opc.Ua.Attributes.Value),
            (NodeId, Opc.Ua.Attributes.ValueRank), (NodeId, Opc.Ua.Attributes.UserAccessLevel)], ct).ConfigureAwait(false);
        var dataType = r[0].Value as NodeId;
        if (r[2].Value is int rank) UaValueRank = rank;
        else Logger.LogWarning("Could not read ValueRank of {Path} ({Status}), assuming scalar", Path, r[2].StatusCode);
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
            var type = await UaUtil.ReadAsync(Session,
                [(dataType, Opc.Ua.Attributes.BrowseName), (dataType, Opc.Ua.Attributes.DataTypeDefinition)], ct).ConfigureAwait(false);
            UaDataType = (type[0].Value as QualifiedName)?.Name ?? "";
            // Custom enum (Python: IntEnum from load_data_type_definitions): value → member name.
            if ((type[1].Value as ExtensionObject)?.Body is EnumDefinition enumDefinition)
                ValidValues = enumDefinition.Fields.ToDictionary(f => f.Value.ToString(CultureInfo.InvariantCulture), f => f.Name);
            else if (StatusCode.IsBad(type[1].StatusCode))
                Logger.LogWarning("Could not load data type definition of {DataType} ({Status})", UaDataType, type[1].StatusCode);
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

        Sample = OpenSmi.Client.Sample.FromDataValue<object?>(r[1], Process);
    }

    public async Task<object?> ReadValueAsync(bool updateCache = false, CancellationToken cancellationToken = default)
    {
        var dv = await ReadDataValueAsync(cancellationToken).ConfigureAwait(false);
        if (updateCache) await OnDataChangeAsync(dv).ConfigureAwait(false);
        return Process(dv.WrappedValue);
    }

    /// <summary>Reads the sample from the server, also with a bad status (no exception).</summary>
    public async Task<Sample<object?>> ReadSampleAsync(bool updateCache = false, CancellationToken cancellationToken = default)
    {
        var dv = (await UaUtil.ReadAsync(Session, [(NodeId, Opc.Ua.Attributes.Value)], cancellationToken).ConfigureAwait(false))[0];
        if (updateCache) await OnDataChangeAsync(dv).ConfigureAwait(false);
        return OpenSmi.Client.Sample.FromDataValue<object?>(dv, Process);
    }

    /// <summary>
    /// Writes <paramref name="value"/> converted to the type of the cached value (e.g. int → Double, string →
    /// NodeId/LocalizedText). A string that is a display name of <see cref="ValidValues"/> (enum member name,
    /// <c>Utilizes</c> target) is written as its key.
    /// </summary>
    public async Task WriteValueAsync(object? value, CancellationToken cancellationToken = default)
    {
        var status = await WriteAsync(ToWriteValue(value), cancellationToken).ConfigureAwait(false);
        if (StatusCode.IsGood(status)) return;
        if (status.Code is StatusCodes.BadUserAccessDenied or StatusCodes.BadRequiresLock or StatusCodes.BadOutOfRange)
            throw OpenSmiStatusException.From(status, $"Write of {Path}");
        throw new OpenSmiInvalidValueException($"{value} is not a valid value for {Path} ({status})");
    }

    internal WriteValue ToWriteValue(object? value)
    {
        if (!IsWritable) throw new OpenSmiInvalidValueException($"{Path} is not writable");
        if (value is string s && !ValidValues.ContainsKey(s) && ValidValues.FirstOrDefault(kv => kv.Value == s).Key is { } key)
            value = key;
        try { return new WriteValue { NodeId = NodeId, AttributeId = Opc.Ua.Attributes.Value, Value = new DataValue(ToVariant(value)) }; }
        catch (FormatException e) { throw new OpenSmiInvalidValueException($"{value} is not a valid value for {Path}", e); }
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

    /// <summary>Raw history (HistoryRead) as samples. <paramref name="numValues"/> 0 = unlimited.</summary>
    public async Task<IReadOnlyList<Sample<object?>>> ReadHistoryAsync(DateTime? start = null, DateTime? end = null,
        uint numValues = 0, bool returnBounds = true, CancellationToken cancellationToken = default)
    {
        // DateTime.MinValue encodes as "unspecified", like asyncua's win epoch default.
        var details = new ExtensionObject(new ReadRawModifiedDetails
        {
            StartTime = start ?? DateTime.MinValue, EndTime = end ?? DateTime.MinValue,
            NumValuesPerNode = numValues, ReturnBounds = returnBounds,
        });
        var result = new List<Sample<object?>>();
        byte[]? continuation = null;
        do
        {
            var response = await Session.HistoryReadAsync(null, details, TimestampsToReturn.Both, false,
                [new HistoryReadValueId { NodeId = NodeId, ContinuationPoint = continuation }], cancellationToken).ConfigureAwait(false);
            var r = response.Results[0];
            OpenSmiStatusException.ThrowIfBad(r.StatusCode, $"History read of {Path}");
            if (ExtensionObject.ToEncodeable(r.HistoryData) is HistoryData data)
                result.AddRange(data.DataValues.Select(dv => OpenSmi.Client.Sample.FromDataValue<object?>(dv, Process)));
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

    /// <summary>Updates the cache; ValueChanged only if the value or its status changed.</summary>
    internal Task OnDataChangeAsync(DataValue dv)
    {
        var (sample, changed) = SampleUpdates.Variable(Sample, dv, Process);
        Sample = sample;
        if (!changed) return Task.CompletedTask;
        _valueWatchers.Publish(sample);
        return ValueChanged.RaiseAsync(this, new(sample), Logger);
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
