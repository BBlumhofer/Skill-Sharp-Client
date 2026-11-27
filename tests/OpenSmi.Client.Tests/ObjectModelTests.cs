namespace OpenSmi.Client.Tests;

/// <summary>The server of <c>servers/paths_server.py</c> and a client of the user developer, fully browsed.</summary>
public sealed class PathsServer : IAsyncLifetime
{
    public TestServer Server { get; private set; } = null!;
    public RemoteServer Client { get; private set; } = null!;

    public RemoteServerOptions Options(string user, IReadOnlyList<BrowseRule>? rules = null) =>
        new() { Url = Server.Url, Username = user, Password = user, BrowseRules = rules, AutoReconnect = false };

    public async Task InitializeAsync()
    {
        Server = await TestServer.StartScriptAsync("paths_server.py", 4874);
        Client = await new RemoteServer(Options("developer")).ConnectAsync();
    }

    public async Task DisposeAsync()
    {
        await Client.DisposeAsync();
        await Server.DisposeAsync();
    }
}

/// <summary>
/// Port of opensmi-client <c>tests/integration/test_paths.py</c> (9026539): paths follow the OpenSMI folders, segments
/// are unique, shared variables are one object with aliases, users belong to the server.
/// </summary>
public class ObjectModelTests(PathsServer fixture) : IClassFixture<PathsServer>
{
    private static readonly BrowseFeature EverythingButSkills = BrowseFeature.All & ~BrowseFeature.SkillSet;
    private RemoteServer Client => fixture.Client;

    [Theory]
    [InlineData("/M1", typeof(RemoteMachine))]
    [InlineData("/M1/Components/Gripper", typeof(RemoteComponent))]
    [InlineData("/M1/Components/Gripper/Components/Finger", typeof(RemoteComponent))]
    [InlineData("/M1/Components/Gripper/SkillSet/Grip", typeof(RemoteSkill))]
    [InlineData("/M1/Resources/Battery", typeof(RemoteResource))]
    [InlineData("/M1/SkillSet/Pick", typeof(RemoteSkill))]
    [InlineData("/M1/SkillSet/Pick/FeasibilityCheck", typeof(RemoteSkillCheck))]
    [InlineData("/M1/SkillSet/Pick/PreconditionCheck", typeof(RemoteSkillCheck))]
    [InlineData("/M1/SkillSet/Pick/ParameterSet/Speed", typeof(RemoteVariable))]
    [InlineData("/M1/MethodSet/Home", typeof(RemoteMethod))]
    [InlineData("/M1/MethodSet/Pick", typeof(RemoteMethod))]
    [InlineData("/M1/Lock", typeof(RemoteLock))]
    [InlineData("/Users/developer", typeof(RemoteUser))]
    [InlineData("/M1/Identification/SerialNumber", typeof(RemoteVariable))]
    public void Paths_follow_the_opensmi_folders(string path, Type type)
    {
        var obj = Client.Resolve(path);

        Assert.IsType(type, obj);
        Assert.Equal(ObjectPath.Parse(path), obj.Path);
    }

    [Fact]
    public void Every_object_has_the_path_and_aliases_of_the_python_client()
    {
        var expected = File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "oracle", "paths-server-objects.txt"));

        Assert.Equal(expected, Client.Find("/**").Select(Describe));
    }

    internal static string Describe(RemoteUaObject obj) =>
        string.Join(' ', [obj.Path.ToString(), obj.GetType().Name,
            .. obj.Aliases.Count > 0 ? [string.Join(',', obj.Aliases.Select(a => a.ToString()).Order(StringComparer.Ordinal))] : Array.Empty<string>()]);

    [Fact]
    public void Skill_and_method_with_the_same_name_have_different_paths()
    {
        var skill = Client.Resolve("/M1/SkillSet/Pick");
        var method = Client.Resolve("/M1/MethodSet/Pick");

        Assert.NotSame(skill, method);
        Assert.Equal("Pick", skill.Name);
        Assert.Equal("Pick", method.Name);
    }

    [Fact]
    public void Checks_lie_below_their_skill()
    {
        var machine = Client.Machines["M1"];
        var skill = machine.SkillSet["Pick"];
        var check = Assert.IsType<RemoteSkillCheck>(machine.FeasibilityCheckSet["Pick"]);

        Assert.Equal("FeasibilityCheck", check.Name);
        Assert.Equal("FeasibilityCheck", check.Path.Name);
        Assert.Same(skill, check.Skill);
        Assert.Equal(skill.Path / "FeasibilityCheck", check.Path);
        Assert.Equal(skill.Path / "PreconditionCheck", machine.PreconditionCheckSet["Pick"].Path);
    }

    [Fact]
    public void Names_from_other_servers_are_sanitized_and_suffixed()
    {
        var monitoring = Client.Machines["M1"].Monitoring;

        Assert.Equal(["Force", "Pick_Place", "Shared", "Temperature", "Temperature_2", "_"], monitoring.Keys.Order(StringComparer.Ordinal));
        Assert.Equal("Pick/Place", monitoring["Pick_Place"].Name);
        Assert.Equal("", monitoring["_"].Name);
        // same browse name, different namespaces: NodeId order decides, by namespace URI first
        Assert.Equal("raw.duplicate", monitoring["Temperature"].NodeId.Identifier);
        Assert.Equal("M1.Monitoring.Temperature", monitoring["Temperature_2"].NodeId.Identifier);
    }

    [Fact]
    public void Collections_are_keyed_by_segment()
    {
        var machine = Client.Machines["M1"];

        Assert.All(machine.Components, kv => Assert.Equal(kv.Key, kv.Value.Path.Name));
        Assert.All(machine.MethodSet, kv => Assert.Equal(kv.Key, kv.Value.Path.Name));
        Assert.Equal(["Home", "Pick"], machine.MethodSet.Keys.Order());
        Assert.Equal(["M1", "M2"], Client.Machines.Keys.Order());
    }

    [Fact]
    public void Shared_variable_is_one_object_with_an_alias()
    {
        var machine = Client.Machines["M1"];
        var force = machine.ParameterSet["Force"];

        Assert.Same(force, machine.Monitoring["Force"]);
        Assert.Equal(ObjectPath.Parse("/M1/ParameterSet/Force"), force.Path); // the server's owner is canonical
        Assert.Equal([ObjectPath.Parse("/M1/Monitoring/Force")], force.Aliases);
        Assert.Same(force, Client.Resolve("/M1/Monitoring/Force"));
        Assert.Single(Client.Find("/M1/**/Force"));
    }

    [Fact]
    public void Variable_owned_by_two_groups_goes_to_the_first_in_node_id_order()
    {
        var shared = Client.Resolve("/M1/Monitoring/Shared");

        Assert.Same(shared, Client.Resolve("/M1/ParameterSet/Shared"));
        Assert.Equal([ObjectPath.Parse("/M1/ParameterSet/Shared")], shared.Aliases);
    }

    [Fact]
    public void Resolve_optional_and_unknown_paths()
    {
        Assert.Null(Client.ResolveOptional("/M1/SkillSet/Drop"));
        Assert.Throws<KeyNotFoundException>(() => Client.Resolve("/M1/SkillSet/Drop"));
        Assert.Equal(ObjectPath.Root, Client.Path);
    }

    [Fact]
    public void Find_skills_everywhere_and_relative_to_an_object()
    {
        Assert.Equal(
            ["/M1/Components/Gripper/Components/Finger/SkillSet/Grip", "/M1/Components/Gripper/SkillSet/Grip", "/M1/SkillSet/Pick"],
            Client.Find("/M1/**/SkillSet/*").Select(s => s.Path.ToString()));
        Assert.All(Client.Find("/M1/**/SkillSet/*"), s => Assert.IsType<RemoteSkill>(s));

        var gripper = Client.Resolve("/M1/Components/Gripper");
        Assert.Same(Client.Resolve("/M1/Components/Gripper/SkillSet/Grip"), gripper.Resolve("SkillSet/Grip"));
        Assert.Null(gripper.ResolveOptional("SkillSet/Drop"));
        Assert.Equal(
            ["/M1/Components/Gripper/Components/Finger/SkillSet/Grip", "/M1/Components/Gripper/SkillSet/Grip"],
            gripper.Find("**/SkillSet/*").Select(s => s.Path.ToString()));
        Assert.Throws<ArgumentException>(() => gripper.Resolve("/M1"));
    }

    [Fact]
    public void Every_object_resolves_by_its_path()
    {
        var objects = Client.Find("/**");

        Assert.True(objects.Count > 50);
        Assert.All(objects, o => Assert.Same(o, Client.Resolve(o.Path)));
    }

    [Fact]
    public async Task String_browse_rules_match()
    {
        BrowseRule[] rules =
        [
            BrowseRules.When(Match.All, EverythingButSkills),
            BrowseRules.When(Match.Path("/M1/Components/Gripper"), BrowseFeature.SkillSet),
            BrowseRules.When(Match.PathPrefix("/M1/Components/Gripper/Components"), BrowseFeature.SkillSet),
            BrowseRules.When(Match.Glob("/M?"), BrowseFeature.SkillSet),
        ];
        await using var client = await new RemoteServer(fixture.Options("operator", rules)).ConnectAsync();
        var machine = client.Machines["M1"];
        var gripper = machine.Components["Gripper"];

        Assert.Equal(["Grip"], gripper.SkillSet.Keys); // Match.Path
        Assert.Equal(["Grip"], gripper.Components["Finger"].SkillSet.Keys); // Match.PathPrefix
        Assert.Equal(["Pick"], machine.SkillSet.Keys); // Match.Glob
    }

    [Fact]
    public async Task Objects_not_matched_by_a_rule_are_not_browsed()
    {
        await using var client = await new RemoteServer(
            fixture.Options("planner", [BrowseRules.When(Match.All, EverythingButSkills)])).ConnectAsync();

        Assert.Empty(client.Find("/**/SkillSet/*"));
    }

    [Fact]
    public void Users_belong_to_the_server()
    {
        var operatorUser = Assert.IsType<RemoteUser>(Client.Resolve("/Users/operator"));

        Assert.Equal(["developer", "operator", "orchestrator", "planner", "remote", "visitor", "watchdog"], Client.Users.Keys.Order());
        Assert.Same(operatorUser, Client.Users["operator"]);
        Assert.Null(operatorUser.Parent);
        Assert.Empty(operatorUser.Aliases);
        Assert.Null(Client.ResolveOptional("/M1/Users/operator"));
        Assert.Equal(20, operatorUser.LockPriority);
    }

    [Fact]
    public async Task Users_follow_the_browse_rules_of_the_root()
    {
        BrowseRule[] rules =
        [
            BrowseRules.When(Match.All, BrowseFeature.All & ~BrowseFeature.Users),
            BrowseRules.When(Match.Path("/M1"), BrowseFeature.Users),
        ];
        await using var client = await new RemoteServer(fixture.Options("remote", rules)).ConnectAsync();

        Assert.Empty(client.Users);
    }

    [Fact]
    public async Task Users_from_the_machines_without_server_wide_folder()
    {
        // Servers before opensmi-server 0.3 list the same user nodes below every machine.
        await using var legacy = await TestServer.StartScriptAsync("paths_server.py", 4875, "legacy");
        await using var client = await new RemoteServer(new RemoteServerOptions
            { Url = legacy.Url, Username = "remote", Password = "remote", AutoReconnect = false }).ConnectAsync();

        var operatorUser = client.Resolve("/Users/operator");
        Assert.Same(operatorUser, client.Users["operator"]);
        Assert.Equal(7, client.Users.Count); // once each, though listed by two machines
        Assert.Null(client.ResolveOptional("/M1/Users/operator"));
    }
}
