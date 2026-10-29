namespace OpenSmi.Client.Tests;

[Collection("server")]
public class WriteTests
{
    [Fact]
    public async Task Write_converts_to_the_variable_type_and_fires_value_changed()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var machine = server.Machines["DummyMachine"];
        await machine.Lock.InitLockAsync();
        var variable = machine.ParameterSet["DummyFloatParameter"];
        var changed = new TaskCompletionSource<object?>();
        variable.ValueChanged.Connect((a, _) => { changed.TrySetResult(a.Value.Value); return Task.CompletedTask; });
        var target = (double)variable.Value! == 2.0 ? 3 : 2;

        await variable.WriteValueAsync(target); // int into a Double variable

        Assert.Equal((double)target, await variable.ReadValueAsync());
        Assert.Equal((double)target, await changed.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Write_all_writes_a_batch_and_rejects_unknown_names_before_writing()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var machine = server.Machines["DummyMachine"];
        await machine.Lock.InitLockAsync();
        var parameters = machine.ParameterSet;

        await parameters.WriteAllAsync(new Dictionary<string, object?> { ["DummyStringParameter"] = "batch", ["DummyIntParameter"] = 7 });
        var values = await parameters.ReadAllAsync();
        Assert.Equal("batch", values["DummyStringParameter"]);
        Assert.Equal(7L, Convert.ToInt64(values["DummyIntParameter"]));

        await Assert.ThrowsAsync<KeyNotFoundException>(() => parameters.WriteAllAsync(
            new Dictionary<string, object?> { ["DummyStringParameter"] = "nope", ["Unknown"] = 1 }));
        Assert.Equal("batch", await parameters["DummyStringParameter"].ReadValueAsync());
    }

    [Fact]
    public async Task History_of_a_historized_monitoring_variable()
    {
        // Only Monitoring is historized on the dummy server; the Python client sees the same (oracle).
        await using var server = await ServerFixture.ConnectAsync();
        var variable = server.Machines["DummyMachine"].Monitoring["RandomValue"];

        var history = await variable.ReadHistoryAsync();

        Assert.NotEmpty(history);
        Assert.All(history, h => Assert.IsType<double>(h.Value));
        Assert.Equal(2, (await variable.ReadHistoryAsync(numValues: 2)).Count);
        Assert.Empty(await server.Machines["DummyMachine"].ParameterSet["DummyStringParameter"].ReadHistoryAsync());
    }

    [Fact]
    public async Task Write_to_read_only_variable_is_invalid()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var skill = server.Machines["DummyMachine"].SkillSet["StartupSkill"];
        await Assert.ThrowsAsync<OpenSmiInvalidValueException>(() =>
            skill.FinalResultData["SuccessfulExecutionsCount"].WriteValueAsync(1));
    }
}
