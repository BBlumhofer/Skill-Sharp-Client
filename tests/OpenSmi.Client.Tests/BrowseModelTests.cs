using Opc.Ua;

namespace OpenSmi.Client.Tests;

/// <summary>Expected values come from the Python client (oracle) against the same dummy server.</summary>
[Collection("server")]
public class BrowseModelTests
{
    [Fact]
    public async Task Browses_machines_components_recursively_and_skills()
    {
        await using var server = await ServerFixture.ConnectAsync();

        var machine = server.Machines["DummyMachine"];
        var component = machine.Components["DummyComponent"];
        Assert.Empty(server.InitExceptions);
        Assert.Empty(machine.InitExceptions);
        Assert.Equal(ObjectPath.Parse("/DummyMachine/Components/DummyComponent/Components/NestedDummyComponent"),
            component.Components["NestedDummyComponent"].Path);
        Assert.Equal(["DummySkillInComponent"], component.SkillSet.Keys);
        Assert.Equal(["DummyMethod"], component.MethodSet.Keys);
        // Python fills its dict in completion order, so only the set is comparable.
        Assert.Equal(
            ["CompositeDummySkill", "ContinuousSkill", "ContinuousSkillSelfHalting", "DummySkill", "DummySkillResources",
             "StartupSkill", "SuspendableDummySkill"],
            machine.SkillSet.Keys.Order());
    }

    [Fact]
    public async Task Variables_carry_metadata_like_the_python_client()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var parameters = server.Machines["DummyMachine"].ParameterSet;

        var f = parameters["DummyFloatParameter"];
        Assert.Equal(ObjectPath.Parse("/DummyMachine/ParameterSet/DummyFloatParameter"), f.Path);
        Assert.Equal("ns=1;s=DummyMachine.ParameterSet.DummyFloatParameter", f.NodeId.ToString());
        Assert.Equal("BaseDataVariableType", f.UaType);
        Assert.Equal("Double", f.UaDataType);
        Assert.True(f.IsWritable);
        Assert.Equal(ValueRanks.Scalar, f.UaValueRank); // Python always reports 0 (SPEC §10)

        var i = parameters["DummyIntParameter"];
        Assert.Equal("AnalogUnitRangeType", i.UaType);
        Assert.Equal("cm", i.Unit?.DisplayName.Text);
        Assert.Equal(100.0, i.Range?.High);

        var skill = server.Machines["DummyMachine"].SkillSet["StartupSkill"];
        Assert.Equal("ns=1;s=DummyMachine.SkillSet.StartupSkill.SkillExecution", skill.NodeId.ToString());
        Assert.Equal(ObjectPath.Parse("/DummyMachine/SkillSet/StartupSkill/FinalResultData/SuccessfulExecutionsCount"),
            skill.FinalResultData["SuccessfulExecutionsCount"].Path);
        Assert.False(skill.FinalResultData["SuccessfulExecutionsCount"].IsWritable);
    }

    [Fact]
    public async Task Every_object_has_the_path_and_aliases_of_the_python_client()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var expected = File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "oracle", "dummy-objects.txt"));

        Assert.Equal(expected, server.Find("/**").Select(ObjectModelTests.Describe));
    }

    [Fact]
    public async Task Browse_rules_limit_features_per_path()
    {
        BrowseRule[] rules =
        [
            BrowseRules.When(Match.All, BrowseFeature.Machines, BrowseFeature.Monitoring),
            BrowseRules.When(Match.Name("DummyMachine"), BrowseFeature.Components, BrowseFeature.SkillSet),
        ];
        await using var server = await ServerFixture.ConnectAsync(rules: rules);

        var machine = server.Machines["DummyMachine"];
        Assert.NotEmpty(machine.SkillSet);
        Assert.Empty(machine.ParameterSet);
        Assert.Throws<InvalidOperationException>(() => machine.Lock);
        Assert.Empty(machine.Components["DummyComponent"].SkillSet);
        Assert.Empty(machine.Components["DummyComponent"].Components);
    }

    [Fact]
    public async Task Empty_rules_skip_discovery_and_allow_direct_node_ids()
    {
        await using var server = await ServerFixture.ConnectAsync(rules: []);
        Assert.Empty(server.Machines);

        var component = await RemoteComponent.CreateAsync(server, "DummyComponent",
            NodeId.Parse("ns=1;s=DummyMachine.Components.DummyComponent"));

        Assert.Equal(ObjectPath.Parse("/Components/DummyComponent"), component.Path); // no parent: the folder below the root
        Assert.Contains("DummySkillInComponent", component.SkillSet.Keys);
        Assert.Same(component, server.GetUaObject(component.NodeId));
    }

    [Fact]
    public async Task States_and_values_are_known_with_status_and_timestamps_after_connect()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var machine = server.Machines["DummyMachine"];

        var state = await machine.ReadCurrentStateAsync();
        Assert.NotNull(state);
        Assert.Equal(state, machine.CurrentState);
        Assert.Equal(Quality.Good, machine.CurrentStateSample!.Quality);
        Assert.Null(machine.Resources.Values.First().CurrentStateSample); // resources have no CurrentState node

        var startup = machine.SkillSet["StartupSkill"];
        Assert.Equal(Quality.Good, startup.CurrentStateSample.Quality); // not waiting for the first notification
        Assert.Equal(await startup.ReadCurrentStateAsync(), startup.CurrentState);

        var parameter = machine.ParameterSet["DummyFloatParameter"];
        Assert.Equal(Quality.Good, parameter.Sample.Quality);
        Assert.NotNull(parameter.Timestamp);
        Assert.Equal(parameter.Value, (await parameter.ReadSampleAsync()).Value);
    }
}
