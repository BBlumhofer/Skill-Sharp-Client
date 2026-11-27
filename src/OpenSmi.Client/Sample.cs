using Opc.Ua;

namespace OpenSmi.Client;

/// <summary>Quality of a <see cref="Sample{T}"/>, from the severity of its status code.</summary>
public enum Quality { Good, Uncertain, Bad }

/// <summary>Machinery item state (OPC UA Machinery <c>MachineryItemState</c>) of a machine or component.</summary>
public enum MachineryItemState
{
    /// <summary>Not available, no activity (e.g. switched off, energy saving mode).</summary>
    NotAvailable,
    /// <summary>Not functional, no activity (e.g. error, blocked).</summary>
    OutOfService,
    /// <summary>Available and functional, waiting for an action from outside.</summary>
    NotExecuting,
    /// <summary>Available, functional and actively performing an activity.</summary>
    Executing,
}

/// <summary>
/// Port of <c>client/sample.py</c>: a single OPC UA value with its status and timestamps. <see cref="Value"/> is the
/// converted value, <c>default</c> (use a nullable <typeparamref name="T"/>) when there is none, e.g. for bad samples.
/// </summary>
public sealed record Sample<T>
{
    public T? Value { get; init; }
    /// <summary>Raw OPC UA value; <c>null</c> when there is none (also for an OPC UA Null variant).</summary>
    public Variant? Variant { get; init; }
    /// <summary>Raw status code; Good by default, like a missing status code in OPC UA.</summary>
    public StatusCode Status { get; init; } = StatusCodes.Good;
    public DateTime? SourceTimestamp { get; init; }
    public DateTime? ServerTimestamp { get; init; }

    public Quality Quality =>
        StatusCode.IsGood(Status) ? Quality.Good : StatusCode.IsUncertain(Status) ? Quality.Uncertain : Quality.Bad;

    /// <summary>Source timestamp, falling back to the server timestamp.</summary>
    public DateTime? Timestamp => SourceTimestamp ?? ServerTimestamp;

    public override string ToString() => $"{Value} ({Quality}{(Quality == Quality.Good ? "" : $" {Status}")}, {Timestamp:O})";
}

public static class Sample
{
    /// <summary>A sample from an OPC UA data value; <paramref name="convert"/> is not called without a value.</summary>
    public static Sample<T> FromDataValue<T>(DataValue dataValue, Func<Variant, T?> convert)
    {
        var variant = dataValue.WrappedValue;
        var hasValue = variant.Value != null;
        return new()
        {
            Value = hasValue ? convert(variant) : default,
            Variant = hasValue ? variant : (Variant?)null,
            Status = dataValue.StatusCode,
            SourceTimestamp = Timestamp(dataValue.SourceTimestamp),
            ServerTimestamp = Timestamp(dataValue.ServerTimestamp),
        };
    }

    private static DateTime? Timestamp(DateTime t) => t == DateTime.MinValue ? null : t;
}

/// <summary>The cache update rules of variables, component states and skill states, as pure functions.</summary>
internal static class SampleUpdates
{
    public static Sample<T> WaitingForInitialData<T>(T? value) => new() { Value = value, Status = StatusCodes.BadWaitingForInitialData };

    /// <summary>Always the new sample; <c>Changed</c> only if value or status changed (the OPC UA default data change trigger).</summary>
    public static (Sample<object?> Sample, bool Changed) Variable(Sample<object?> old, DataValue dv, Func<Variant, object?> process)
    {
        var sample = Sample.FromDataValue<object?>(dv, process);
        return (sample, !Equals(sample.Variant, old.Variant) || sample.Status != old.Status);
    }

    /// <summary><c>null</c> to ignore the update: an empty value with a usable status (keeps the last state) or an unknown state.</summary>
    public static (Sample<MachineryItemState?> Sample, bool Changed)? ComponentState(Sample<MachineryItemState?> old, DataValue dv)
    {
        Sample<MachineryItemState?> sample;
        try { sample = Sample.FromDataValue(dv, v => ParseItemState(v.Value)); }
        catch (OpenSmiUnknownStateException) { return null; }
        if (sample.Value == null && sample.Quality != Quality.Bad) return null;
        return (sample, sample.Value != old.Value || sample.Status != old.Status);
    }

    /// <summary>
    /// <c>null</c> to ignore an unknown state. Without a value (e.g. bad status) the skill is assumed Halted.
    /// <c>StateChanged</c>: the state itself changed, not only its status.
    /// </summary>
    public static (Sample<SkillState> Sample, bool Changed, bool StateChanged)? SkillState(Sample<SkillState> old, DataValue dv)
    {
        Sample<SkillState> sample;
        try { sample = Sample.FromDataValue(dv, v => RemoteSkill.ParseState(StateText(v.Value))); }
        catch (OpenSmiUnknownStateException) { return null; }
        if (sample.Variant == null) sample = sample with { Value = Client.SkillState.Halted };
        var stateChanged = sample.Value != old.Value;
        return (sample, stateChanged || sample.Status != old.Status, stateChanged);
    }

    /// <summary>Ignores case, spaces and underscores (<c>Not Executing</c>, <c>NOT_EXECUTING</c>); <c>null</c> for empty values.</summary>
    /// <exception cref="OpenSmiUnknownStateException">A non-empty value that is no <see cref="MachineryItemState"/>.</exception>
    public static MachineryItemState? ParseItemState(object? value)
    {
        var text = StateText(value);
        if (text.Length == 0) return null;
        var key = text.Replace(" ", "").Replace("_", "");
        return Enum.GetValues<MachineryItemState>().Cast<MachineryItemState?>()
            .FirstOrDefault(s => string.Equals(s.ToString(), key, StringComparison.OrdinalIgnoreCase))
            ?? throw new OpenSmiUnknownStateException($"Unknown machinery item state '{text}'");
    }

    /// <summary>Some servers send plain strings instead of LocalizedText.</summary>
    public static string StateText(object? value) => value switch
    {
        LocalizedText lt => lt.Text ?? "",
        null => "",
        _ => value.ToString() ?? "",
    };
}
