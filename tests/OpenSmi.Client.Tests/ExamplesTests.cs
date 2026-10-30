using OpenSmi.Client.Examples;

namespace OpenSmi.Client.Tests;

public sealed class ExamplesServer : IAsyncLifetime
{
    public TestServer Server { get; private set; } = null!;
    public async Task InitializeAsync() => Server = await TestServer.StartAsync(4872);
    public async Task DisposeAsync() => await Server.DisposeAsync();
}

/// <summary>Runs every example entry point against a private dummy server and checks what it prints.</summary>
public class ExamplesTests(ExamplesServer fixture) : IClassFixture<ExamplesServer>
{
    private string Url => fixture.Server.Url;

    /// <summary>Captures stdout; safe because test parallelization is disabled (ServerFixture.cs).</summary>
    internal static async Task<string> OutputOfAsync(Func<Task> example)
    {
        await ServerFixture.WaitForClosedSessionsAsync();
        var original = Console.Out;
        var output = new StringWriter();
        Console.SetOut(output);
        try { await example(); }
        finally { Console.SetOut(original); }
        return output.ToString();
    }

    [Fact]
    public async Task Dto_prints_machine_as_json()
    {
        var output = await OutputOfAsync(() => DtoExample.RunAsync(Url));
        Assert.Contains("\"Path\": \"/DummyMachine\"", output);
    }

    [Fact]
    public async Task Cold_start_brings_up_startup_skill_and_runs_dummy_skill()
    {
        var output = await OutputOfAsync(() => ColdStartExample.RunAsync(Url));
        Assert.Contains("Skill `DummySkill` now in state `Completed`", output);
        Assert.Contains("results:", output);
    }

    [Fact]
    public async Task Quick_start_runs_a_skill()
    {
        await ServerFixture.WaitForClosedSessionsAsync();
        await ColdStartExample.RunAsync(Url); // DummySkill needs the StartupSkill running
        var output = await OutputOfAsync(() => QuickStartExample.RunAsync(Url, "DummyMachine", "DummySkill"));
        Assert.Contains("results:", output);
    }

    [Fact]
    public async Task Method_call_prints_results()
    {
        var output = await OutputOfAsync(() => MethodCallExample.RunAsync(Url));
        Assert.Contains("results:", output);
    }

    [Fact]
    public async Task History_prints_the_last_values()
    {
        var output = await OutputOfAsync(() => HistoryExample.RunAsync(Url));
        Assert.StartsWith("RandomValue=", output);
        Assert.InRange(output.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length, 2, 11);
    }

    [Fact]
    public async Task Browse_rules_browse_components_only_where_they_match()
    {
        var output = await OutputOfAsync(() => BrowseRulesExample.RunAsync(Url));
        Assert.Contains("\"Path\": \"/DummyMachine/DummyComponent\"", output);
        Assert.DoesNotContain("\"Path\": \"/DummyMachine/Lock\"", output);
    }

    [Fact]
    public async Task Direct_component_is_built_from_its_node_id()
    {
        var output = await OutputOfAsync(() => DirectComponentExample.RunAsync(Url));
        Assert.Contains("\"Path\": \"/DummyComponent\"", output);
        Assert.Contains("DummySkillInComponent", output);
    }

    [Fact]
    public async Task Auto_lock_holds_the_lock_for_the_duration()
    {
        var output = await OutputOfAsync(() => AutoLockExample.RunAsync(Url, TimeSpan.FromSeconds(2)));
        Assert.Contains("locked by orchestrator", output);
    }

    [Fact]
    public async Task Subscription_server_prints_status_and_server_time()
    {
        var output = await OutputOfAsync(() => SubscriptionServerExample.RunAsync(Url, TimeSpan.FromSeconds(2)));
        Assert.Contains("received server time", output);
        Assert.Contains("new status: Disconnecting", output);
    }

    [Fact]
    public async Task Subscription_variable_prints_value_changes()
    {
        var output = await OutputOfAsync(() => SubscriptionVariableExample.RunAsync(Url, TimeSpan.FromSeconds(2)));
        Assert.Contains("new value: /DummyMachine/DummyComponent/Monitoring/", output);
    }

    [Fact]
    public async Task Subscription_component_runs_for_the_duration()
    {
        // The dummy machine only notifies while skills run, so this checks that it connects and stops cleanly after the duration.
        await ServerFixture.WaitForClosedSessionsAsync();
        await SubscriptionComponentExample.RunAsync(Url, TimeSpan.FromSeconds(2));
    }
}

public sealed class EncryptedExamplesServer : IAsyncLifetime
{
    public TestServer Server { get; private set; } = null!;
    public async Task InitializeAsync() => Server = await TestServer.StartAsync(4873, encryption: true);
    public async Task DisposeAsync() => await Server.DisposeAsync();
}

public class SecurityPolicyExampleTests(EncryptedExamplesServer fixture) : IClassFixture<EncryptedExamplesServer>
{
    [Fact]
    public async Task Security_policy_connects_encrypted_and_prints_the_machine()
    {
        var store = Directory.CreateTempSubdirectory("opensmi-pki-").FullName;
        var output = await ExamplesTests.OutputOfAsync(() =>
            SecurityPolicyExample.RunAsync(fixture.Server.Url, store, autoAcceptServerCertificate: true));
        Assert.Contains("\"Path\": \"/DummyMachine\"", output);
    }
}
