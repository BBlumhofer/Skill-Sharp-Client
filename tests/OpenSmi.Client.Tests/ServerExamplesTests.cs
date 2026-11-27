#if NET10_0_OR_GREATER
using OpenSmi.Client.Examples;
#endif

namespace OpenSmi.Client.Tests;

/// <summary>
/// The examples of opensmi-server (check, dictionary_entries, quick_start, rsl; dummy runs in the other tests), each
/// started unchanged on its own port when first used.
/// </summary>
public sealed class ServerExamples : IAsyncLifetime
{
    private static readonly Dictionary<string, int> Ports = new()
    {
        ["check"] = 4880, ["dictionary_entries"] = 4881, ["quick_start"] = 4882, ["rsl"] = 4883,
    };
    private readonly Dictionary<string, Task<TestServer>> _servers = [];

    public static IEnumerable<object[]> Names => Ports.Keys.Select(n => new object[] { n });

    public Task<TestServer> GetAsync(string name)
    {
        lock (_servers)
            return _servers.TryGetValue(name, out var server) ? server : _servers[name] = TestServer.StartExampleAsync(name, Ports[name]);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var server in _servers.Values) await (await server).DisposeAsync();
    }
}

/// <summary>Verifies the client against every opensmi-server example: the browsed objects and what the example is for.</summary>
public class ServerExamplesTests(ServerExamples examples) : IClassFixture<ServerExamples>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private async Task<RemoteServer> ConnectAsync(string example, string user = "developer") =>
        await new RemoteServer(new RemoteServerOptions
        {
            Url = (await examples.GetAsync(example)).Url, Username = user, Password = user, AutoReconnect = false,
        }).ConnectAsync();

    [Theory]
    [MemberData(nameof(ServerExamples.Names), MemberType = typeof(ServerExamples))]
    public async Task Browses_the_same_objects_as_the_python_client(string example)
    {
        await using var client = await ConnectAsync(example);
        var expected = File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "oracle", $"example-{example}-objects.txt"));

        Assert.Empty(client.InitExceptions);
        Assert.Equal(expected, client.Find("/**").Select(ObjectModelTests.Describe));
        // like the Python client: the examples have no startup skill, so their machine is out of service
        Assert.Equal(MachineryItemState.OutOfService, client.Machines["ExampleMachine"].CurrentState);
    }

#if NET10_0_OR_GREATER // the examples project targets net10.0 only
    [Fact]
    public async Task Quick_start_example_runs_the_example_skill()
    {
        var url = (await examples.GetAsync("quick_start")).Url;

        var output = await ExamplesTests.OutputOfAsync(() => QuickStartExample.RunAsync(url));

        Assert.Contains("ComputationResult=15", output); // X=5 + Y=10
    }
#endif

    [Fact]
    public async Task Check_example_runs_the_skill_with_the_id_of_its_feasibility_check()
    {
        await using var client = await ConnectAsync("check", "operator");
        var machine = client.Machines["ExampleMachine"];
        await machine.Lock.InitLockAsync();
        var check = machine.FeasibilityCheckSet["FullSkill"];
        var skill = (RemoteSkill)client.Resolve("/ExampleMachine/SkillSet/FullSkill");
        Assert.Same(skill, check.Skill);

        await ResetAsync(check);
        await check.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["X"] = 2, ["Y"] = 3 });
        await check.StartAsync(StartWait.Completion, Timeout);
        var feasibilityId = check.FinalResultData["FeasibilityID"].Value;

        await ResetAsync(skill);
        await skill.ParameterSet["FeasibilityID"].WriteValueAsync(feasibilityId);
        await skill.StartAsync(StartWait.Completion, Timeout);

        Assert.Equal(5, Convert.ToInt32(await skill.FinalResultData["ComputationResult"].ReadValueAsync()));
    }

    [Fact]
    public async Task Check_example_halts_an_infeasible_check_with_its_error_code()
    {
        await using var client = await ConnectAsync("check", "operator");
        var machine = client.Machines["ExampleMachine"];
        await machine.Lock.InitLockAsync();
        var notification = new TaskCompletionSource<NotificationEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        machine.NotificationReceived += (_, e) =>
        {
            if (e.Notification.ErrorCode == "E9435") notification.TrySetResult(e.Notification);
            return Task.CompletedTask;
        };
        var check = machine.FeasibilityCheckSet["FullSkill"];

        await ResetAsync(check);
        await check.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["X"] = 2, ["Y"] = 2 });

        await Assert.ThrowsAsync<SkillHaltedException>(() => check.StartAsync(StartWait.Completion, Timeout));
        Assert.Contains("even number", (await notification.Task.WaitAsync(Timeout)).Message);
    }

    [Fact]
    public async Task Dictionary_entries_example_provides_the_fax_number()
    {
        await using var client = await ConnectAsync("dictionary_entries");
        var fax = client.Machines["ExampleMachine"].ParameterSet["FaxNumber"];

        Assert.Equal("+49.631.12345-6789", fax.Value);
        Assert.Equal(Quality.Good, fax.Sample.Quality);
        Assert.Equal("+49.631.12345-6789", await fax.ReadValueAsync());
    }

    [Fact]
    public async Task Rsl_example_browses_the_component_with_its_spatial_object()
    {
        await using var client = await ConnectAsync("rsl");
        var component = (RemoteComponent)client.Resolve("/ExampleMachine/Components/DummyComponent");

        Assert.Same(component, client.Machines["ExampleMachine"].Components["DummyComponent"]);
        Assert.Equal("1234-56789-abc", component.Identification["SerialNumber"].Value);
        Assert.Null(component.CurrentStateSample); // no MachineryItemState node, like in the Python client
    }

    private static async Task ResetAsync(RemoteSkill skill)
    {
        var state = await skill.ReadCurrentStateAsync();
        if (state == SkillState.Ready) return;
        if (state is not (SkillState.Halted or SkillState.Suspended or SkillState.Completed))
            await skill.HaltAsync(wait: true, Timeout);
        await skill.ResetAsync(wait: true, Timeout);
    }
}
