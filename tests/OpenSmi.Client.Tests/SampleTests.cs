using Opc.Ua;

namespace OpenSmi.Client.Tests;

/// <summary>
/// Port of opensmi-client <c>tests/unit/test_sample.py</c> and the update rules of <c>test_remote_variable.py</c>,
/// <c>test_remote_component.py</c> and <c>test_remote_skill.py</c> (9026539).
/// </summary>
public class SampleTests
{
    private static readonly DateTime SourceTime = new(2026, 10, 31, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime ServerTime = new(2026, 10, 31, 12, 0, 1, DateTimeKind.Utc);

    private static DataValue Dv(object? value, uint status = StatusCodes.Good, DateTime? time = null) =>
        new(value == null ? Variant.Null : new Variant(value), status, time ?? SourceTime, ServerTime);

    [Fact]
    public void Defaults()
    {
        var sample = new Sample<object?>();

        Assert.Null(sample.Value);
        Assert.Null(sample.Variant);
        Assert.True(StatusCode.IsGood(sample.Status));
        Assert.Equal(Quality.Good, sample.Quality);
        Assert.Null(sample.Timestamp);
    }

    [Theory]
    [InlineData(StatusCodes.Good, Quality.Good)]
    [InlineData(StatusCodes.UncertainLastUsableValue, Quality.Uncertain)]
    [InlineData(StatusCodes.BadWaitingForInitialData, Quality.Bad)]
    public void Quality_follows_the_status_severity(uint status, Quality quality) =>
        Assert.Equal(quality, new Sample<object?> { Status = status }.Quality);

    [Fact]
    public void Timestamp_prefers_the_source()
    {
        Assert.Equal(SourceTime, new Sample<object?> { SourceTimestamp = SourceTime, ServerTimestamp = ServerTime }.Timestamp);
        Assert.Equal(ServerTime, new Sample<object?> { ServerTimestamp = ServerTime }.Timestamp);
    }

    [Fact]
    public void From_data_value_converts_the_variant()
    {
        var sample = Sample.FromDataValue(Dv(42), v => (object?)$"{v.Value}");

        Assert.Equal("42", sample.Value);
        Assert.Equal(new Variant(42), sample.Variant);
        Assert.Equal(Quality.Good, sample.Quality);
        Assert.Equal(SourceTime, sample.SourceTimestamp);
        Assert.Equal(ServerTime, sample.ServerTimestamp);
    }

    [Fact]
    public void From_data_value_without_value_does_not_convert()
    {
        var sample = Sample.FromDataValue<object?>(new DataValue(Variant.Null, StatusCodes.BadCommunicationError),
            _ => throw new InvalidOperationException("convert called without variant"));

        Assert.Null(sample.Value);
        Assert.Null(sample.Variant);
        Assert.Equal(Quality.Bad, sample.Quality);
        Assert.Equal(StatusCodes.BadCommunicationError, sample.Status.Code);
        Assert.Null(sample.SourceTimestamp); // DateTime.MinValue means "not sent"
    }

    [Fact]
    public void Variable_update_signals_value_or_status_changes_only()
    {
        var initial = SampleUpdates.WaitingForInitialData<object?>(null);
        Assert.Equal(StatusCodes.BadWaitingForInitialData, initial.Status.Code);

        var (first, changed) = SampleUpdates.Variable(initial, Dv(42), v => v.Value);
        Assert.True(changed);
        Assert.Equal(42, first.Value);
        Assert.Equal(SourceTime, first.Timestamp);

        var (later, timestampOnly) = SampleUpdates.Variable(first, Dv(42, time: ServerTime), v => v.Value);
        Assert.False(timestampOnly); // cache updated, no signal
        Assert.Equal(ServerTime, later.Timestamp);

        var (uncertain, statusOnly) = SampleUpdates.Variable(later, Dv(42, StatusCodes.UncertainLastUsableValue), v => v.Value);
        Assert.True(statusOnly);
        Assert.Equal(Quality.Uncertain, uncertain.Quality);

        var (bad, _) = SampleUpdates.Variable(uncertain, new DataValue(Variant.Null, StatusCodes.BadCommunicationError), v => v.Value);
        Assert.Null(bad.Value);
        Assert.Null(bad.Variant);
    }

    [Theory]
    [InlineData("Executing", MachineryItemState.Executing)]
    [InlineData("NotExecuting", MachineryItemState.NotExecuting)]
    [InlineData("Not Executing", MachineryItemState.NotExecuting)]
    [InlineData("NOT_EXECUTING", MachineryItemState.NotExecuting)]
    [InlineData("outofservice", MachineryItemState.OutOfService)]
    [InlineData("NotAvailable", MachineryItemState.NotAvailable)]
    public void Item_state_ignores_case_spaces_and_underscores(string text, MachineryItemState expected)
    {
        Assert.Equal(expected, SampleUpdates.ParseItemState(text));
        Assert.Equal(expected, SampleUpdates.ParseItemState(new LocalizedText(text)));
    }

    [Fact]
    public void Empty_item_state_is_null_and_unknown_throws()
    {
        Assert.Null(SampleUpdates.ParseItemState(""));
        Assert.Null(SampleUpdates.ParseItemState(null));
        Assert.Null(SampleUpdates.ParseItemState(new LocalizedText("")));
        Assert.Throws<OpenSmiUnknownStateException>(() => SampleUpdates.ParseItemState("Unknown"));
    }

    [Fact]
    public void Component_state_ignores_empty_and_unknown_values_but_not_bad_status()
    {
        var initial = SampleUpdates.WaitingForInitialData<MachineryItemState?>(null);
        var (executing, changed) = SampleUpdates.ComponentState(initial, Dv(new LocalizedText("Executing")))!.Value;
        Assert.True(changed);
        Assert.Equal(MachineryItemState.Executing, executing.Value);

        Assert.Null(SampleUpdates.ComponentState(executing, Dv(null)));
        Assert.Null(SampleUpdates.ComponentState(executing, Dv("")));
        Assert.Null(SampleUpdates.ComponentState(executing, Dv(new LocalizedText(""))));
        Assert.Null(SampleUpdates.ComponentState(executing, Dv("Unknown")));

        var (bad, badChanged) = SampleUpdates.ComponentState(executing, new DataValue(Variant.Null, StatusCodes.BadCommunicationError))!.Value;
        Assert.True(badChanged);
        Assert.Null(bad.Value);
        Assert.Equal(Quality.Bad, bad.Quality);

        var (uncertain, statusOnly) = SampleUpdates.ComponentState(executing, Dv("Executing", StatusCodes.UncertainLastUsableValue))!.Value;
        Assert.True(statusOnly);
        Assert.Equal(MachineryItemState.Executing, uncertain.Value);
        Assert.False(SampleUpdates.ComponentState(uncertain, Dv("Executing", StatusCodes.UncertainLastUsableValue, ServerTime))!.Value.Changed);
    }

    [Fact]
    public void Skill_state_assumes_halted_without_value_and_ignores_unknown_states()
    {
        var initial = SampleUpdates.WaitingForInitialData(SkillState.Halted);
        Assert.Equal(SkillState.Halted, initial.Value);

        var ready = SampleUpdates.SkillState(initial, Dv(new LocalizedText("Ready")))!.Value;
        Assert.Equal(SkillState.Ready, ready.Sample.Value);
        Assert.True(ready.Changed);
        Assert.True(ready.StateChanged);

        Assert.Null(SampleUpdates.SkillState(ready.Sample, Dv("Unknown")));

        var bad = SampleUpdates.SkillState(ready.Sample, new DataValue(Variant.Null, StatusCodes.BadCommunicationError))!.Value;
        Assert.Equal(SkillState.Halted, bad.Sample.Value);
        Assert.Equal(Quality.Bad, bad.Sample.Quality);
        Assert.True(bad.StateChanged);

        var statusOnly = SampleUpdates.SkillState(bad.Sample, Dv("Halted", StatusCodes.UncertainLastUsableValue))!.Value;
        Assert.True(statusOnly.Changed);
        Assert.False(statusOnly.StateChanged); // no second halt
    }
}
