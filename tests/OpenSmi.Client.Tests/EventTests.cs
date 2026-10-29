namespace OpenSmi.Client.Tests;

public class EventTests
{
    private static RemoteServer Unreachable() =>
        new(new RemoteServerOptions { Url = "opc.tcp://localhost:4999/server", Username = "u", Password = "p" });

    [Fact]
    public async Task Failing_handler_does_not_stop_the_next_one_and_handlers_run_in_subscription_order()
    {
        var server = Unreachable();
        var calls = new List<string>();
        server.StatusChanged += (_, e) => { calls.Add($"first {e.Status}"); throw new InvalidOperationException("boom"); };
        server.StatusChanged += async (sender, e) =>
        {
            await Task.Yield();
            Assert.Same(server, sender);
            calls.Add($"second {e.Status}");
        };

        await Assert.ThrowsAsync<OpenSmiConnectionException>(() => server.ConnectAsync(new ConnectRetryOptions(MaxRetries: 1)));

        Assert.Equal(["first Connecting", "second Connecting", "first Disconnected", "second Disconnected"], calls);
    }

    [Fact]
    public async Task Watch_yields_the_current_value_first()
    {
        var server = Unreachable();
        await using var watch = server.WatchStatusAsync().GetAsyncEnumerator();
        Assert.True(await watch.MoveNextAsync());
        Assert.Equal(RemoteServerStatus.Disconnected, watch.Current);
    }

    [Fact]
    public async Task Slow_watcher_loses_old_values_but_gets_the_newest()
    {
        var watchers = new Watchers<int>();
        await using var watch = watchers.WatchAsync(() => 0).GetAsyncEnumerator();
        Assert.True(await watch.MoveNextAsync());
        Assert.Equal(0, watch.Current);

        for (var i = 1; i <= 100; i++) watchers.Publish(i);

        var received = new List<int>();
        while (await watch.MoveNextAsync())
        {
            received.Add(watch.Current);
            if (watch.Current == 100) break;
        }
        Assert.True(received.Count < 100, string.Join(",", received));
        Assert.Equal(100, received[^1]);
    }

    [Fact]
    public async Task Watch_unsubscribes_when_enumeration_ends()
    {
        var watchers = new Watchers<int>();
        await foreach (var _ in watchers.WatchAsync(() => 0)) break;
        Assert.Equal(0, watchers.Count);
    }
}
