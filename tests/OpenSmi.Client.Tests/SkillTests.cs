using Opc.Ua;

namespace OpenSmi.Client.Tests;

public class SkillStateParsingTests
{
    [Theory]
    [InlineData("", SkillState.Halted)]
    [InlineData("Running", SkillState.Running)]
    [InlineData("COMPLETED", SkillState.Completed)]
    public void Parses_state_text(string text, SkillState expected) => Assert.Equal(expected, RemoteSkill.ParseState(text));

    [Theory]
    [InlineData("Bogus")]
    [InlineData("13")]
    public void Unknown_state_text_is_a_protocol_error(string text) =>
        Assert.Throws<OpenSmiProtocolException>(() => RemoteSkill.ParseState(text));

    [Fact]
    public void Notification_error_code_from_event_wins_over_message_suffix()
    {
        Assert.Equal(new NotificationEvent("Fehler", 500, "E42"), NotificationEvent.Parse("Fehler|||E42", 500, null));
        Assert.Equal(new NotificationEvent("Fehler", 500, "E1"), NotificationEvent.Parse("Fehler|||E42", 500, "E1"));
        Assert.Equal(new NotificationEvent("Info", 100, ""), NotificationEvent.Parse("Info", 100, ""));
        Assert.Null(NotificationEvent.Parse(null, 100, "E1"));
    }
}

[Collection("server")]
public class SkillTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    /// <summary>Lock the machine and bring up the StartupSkill, as opensmi-client/examples/example.py does.</summary>
    private static async Task<RemoteMachine> ReadyMachineAsync(RemoteServer server)
    {
        var machine = server.Machines["DummyMachine"];
        await machine.Lock.InitLockAsync();
        var startup = machine.SkillSet["StartupSkill"];
        if (await startup.ReadCurrentStateAsync() != SkillState.Running)
        {
            await EnsureReadyAsync(startup);
            await startup.StartAsync(StartWait.RunningOrCompleted, Timeout);
        }
        return machine;
    }

    private static async Task EnsureReadyAsync(RemoteSkill skill)
    {
        var state = await skill.ReadCurrentStateAsync();
        if (state == SkillState.Ready) return;
        if (state is not (SkillState.Halted or SkillState.Suspended or SkillState.Completed))
            await skill.HaltAsync(wait: true, Timeout);
        await skill.ResetAsync(wait: true, Timeout);
    }

    [Fact]
    public async Task Finite_skill_runs_to_completion()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var skill = (await ReadyMachineAsync(server)).SkillSet["DummySkill"];
        var states = new List<SkillState>();
        skill.StateChanged.Connect((a, _) => { lock (states) states.Add(a.State); return Task.CompletedTask; });

        Assert.True(skill.IsFinite);
        await EnsureReadyAsync(skill);
        await skill.StartAsync(StartWait.Completion, Timeout);

        Assert.True(skill.CurrentState == SkillState.Completed, string.Join(",", states));
        Assert.Contains(SkillState.Running, states);
        Assert.Equal(SkillState.Completed, await skill.ReadCurrentStateAsync());
    }

    [Fact]
    public async Task Skill_run_delivers_notifications()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var machine = await ReadyMachineAsync(server);
        var received = new TaskCompletionSource<NotificationEvent>();
        machine.NotificationReceived.Connect((a, _) =>
        {
            if (a.Event.Message.Contains("operation mode")) received.TrySetResult(a.Event); // dummy_skill.py:71
            return Task.CompletedTask;
        });
        var skill = machine.SkillSet["DummySkill"];
        await EnsureReadyAsync(skill);

        await skill.StartAsync(StartWait.Completion, Timeout);

        var notification = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("", notification.ErrorCode);
    }

    [Fact]
    public async Task Completion_wait_is_rejected_for_continuous_skills()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var skill = server.Machines["DummyMachine"].SkillSet["ContinuousSkill"];
        Assert.False(skill.IsFinite);
        await Assert.ThrowsAsync<InvalidOperationException>(() => skill.StartAsync(StartWait.Completion));
    }

    [Fact]
    public async Task Halt_during_wait_raises_skill_halted()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var skill = (await ReadyMachineAsync(server)).SkillSet["ContinuousSkill"];
        await EnsureReadyAsync(skill);
        await skill.StartAsync(StartWait.RunningOrCompleted, Timeout);

        var wait = skill.WaitForStateAsync(SkillState.Completed, Timeout);
        await skill.HaltAsync();

        await Assert.ThrowsAsync<SkillHaltedException>(() => wait);
    }

    [Fact]
    public async Task Suspend_only_on_suspendable_skills()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var skills = server.Machines["DummyMachine"].SkillSet;
        Assert.True(skills["SuspendableDummySkill"].IsSuspendable);
        await Assert.ThrowsAsync<SkillNotSuspendableException>(() => skills["DummySkill"].SuspendAsync());
    }

    [Fact]
    public async Task Call_without_lock_is_access_denied()
    {
        // Methods have no state machine, so the server's lock check is not preceded by a state check.
        await using var server = await ServerFixture.ConnectAsync();
        await Assert.ThrowsAsync<OpenSmiAccessDeniedException>(() => server.Machines["DummyMachine"].MethodSet.Values.First().CallAsync());
    }

    [Fact]
    public async Task Start_in_wrong_state_is_invalid_state()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var skill = (await ReadyMachineAsync(server)).SkillSet["DummySkill"];
        await EnsureReadyAsync(skill);
        await skill.HaltAsync(wait: true, Timeout);
        await Assert.ThrowsAsync<OpenSmiInvalidStateException>(() => skill.StartAsync());
    }

    [Fact]
    public async Task Method_call_returns_final_result_data()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var machine = await ReadyMachineAsync(server);
        var method = machine.MethodSet.Values.First();

        var result = await method.CallAsync();

        Assert.Equal(method.FinalResultData.Keys.Order(), result.Keys.Order());
        Assert.NotNull(method.MinAccessLevel);
    }

    [Fact]
    public async Task Requirements_give_dependencies()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var skills = server.Machines["DummyMachine"].SkillSet;
        Assert.Equal([NodeId.Parse("ns=1;s=DummyMachine.SkillSet.DummySkill.SkillExecution")], skills["CompositeDummySkill"].DependsOn);
        Assert.Empty(skills["DummySkill"].DependsOn);
    }

    [Fact]
    public async Task Users_expose_lock_priority()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var orchestrator = server.Machines["DummyMachine"].Users.Single(u => u.Name == "orchestrator");
        Assert.Equal("10", orchestrator.Data.UserLevel);
        Assert.Equal(10, orchestrator.LockPriority);
        Assert.True(orchestrator.Data.IsPresent);
    }

    [Fact]
    public async Task Auto_lock_relocks_after_another_user_released_it()
    {
        BrowseRule[] lockOnly = [BrowseRules.When(Match.All, BrowseFeature.Machines, BrowseFeature.Lock)];
        await using var orchestrator = await ServerFixture.ConnectAsync(rules: lockOnly);
        await using var operatorServer = await ServerFixture.ConnectAsync("operator", lockOnly);
        var ownLock = orchestrator.Machines["DummyMachine"].Lock;
        await using var autoLock = new AutoLock(ownLock, interval: TimeSpan.FromMilliseconds(200));

        await autoLock.LockAsync();
        await WaitUntilAsync(() => ownLock.IsLockedByUs);
        await operatorServer.Machines["DummyMachine"].Lock.BreakLockAsync();
        await WaitUntilAsync(() => ownLock.LockingUser == "operator");
        await operatorServer.Machines["DummyMachine"].Lock.ExitLockAsync();

        await WaitUntilAsync(() => ownLock.IsLockedByUs);
        await autoLock.UnlockAsync();
        await WaitUntilAsync(() => !ownLock.IsLocked);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "condition not reached within 10 s");
            await Task.Delay(50);
        }
    }
}
