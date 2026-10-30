namespace OpenSmi.Client.Tests;

[Collection("server")]
public class LockTests
{
    private static readonly BrowseRule[] LockOnly = [BrowseRules.When(Match.All, BrowseFeature.Machines, BrowseFeature.Lock)];

    [Fact]
    public async Task Init_and_exit_lock()
    {
        await using var server = await ServerFixture.ConnectAsync(rules: LockOnly);
        var remoteLock = server.Machines["DummyMachine"].Lock;

        Assert.Equal(LockResult.Ok, await remoteLock.InitLockAsync());
        var state = await remoteLock.ReadStateAsync();
        Assert.True(state.LockedByUs);
        Assert.True(remoteLock.IsLockedByUs);
        Assert.Equal("orchestrator", remoteLock.LockingUser);
        Assert.NotNull(remoteLock.RemainingLockTimeMs);

        Assert.Equal(LockResult.Ok, await remoteLock.ExitLockAsync());
        Assert.False((await remoteLock.ReadStateAsync()).Locked);
        // BreakLock on a free lock takes it and reports E_NotLocked (opensmi-server lock.py:_ua_break_lock)
        Assert.Equal(LockResult.NotLocked, await remoteLock.BreakLockAsync());
        Assert.Equal(LockResult.Ok, await remoteLock.ExitLockAsync());
    }

    [Fact]
    public async Task Higher_priority_user_breaks_the_lock()
    {
        await using var orchestrator = await ServerFixture.ConnectAsync(rules: LockOnly);
        await using var operatorServer = await ServerFixture.ConnectAsync("operator", LockOnly);
        var ownLock = orchestrator.Machines["DummyMachine"].Lock;
        var taken = new TaskCompletionSource();
        ownLock.LockingUserChanged += (_, e) =>
        {
            if (e.User == "operator") taken.TrySetResult();
            return Task.CompletedTask;
        };

        await ownLock.InitLockAsync();
        Assert.Equal(LockResult.Ok, await operatorServer.Machines["DummyMachine"].Lock.BreakLockAsync()); // steals the lock

        await taken.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(ownLock.IsLockedByUs);
        await Assert.ThrowsAsync<OpenSmiAccessDeniedException>(() => ownLock.BreakLockAsync());
        await operatorServer.Machines["DummyMachine"].Lock.ExitLockAsync();
    }
}
