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
        Assert.Equal("/DummyMachine/DummyComponent/NestedDummyComponent", component.Components["NestedDummyComponent"].Path);
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
        Assert.Equal("/DummyMachine/ParameterSet/DummyFloatParameter", f.Path);
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
        Assert.Equal("/DummyMachine/StartupSkill/FinalResultData/SuccessfulExecutionsCount",
            skill.FinalResultData["SuccessfulExecutionsCount"].Path);
        Assert.False(skill.FinalResultData["SuccessfulExecutionsCount"].IsWritable);
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

        Assert.Equal("/DummyComponent", component.Path);
        Assert.Contains("DummySkillInComponent", component.SkillSet.Keys);
        Assert.Same(component, server.GetUaObject(component.NodeId));
    }

    [Fact]
    public async Task Component_state_is_read_and_cached()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var machine = server.Machines["DummyMachine"];
        var state = await machine.ReadCurrentStateAsync();
        Assert.NotEqual("Unknown", state);
        // The cache is filled by the subscription's first notification, which may arrive after ConnectAsync.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (machine.CurrentState != state && DateTime.UtcNow < deadline) await Task.Delay(50);
        Assert.Equal(state, machine.CurrentState);
    }
}
