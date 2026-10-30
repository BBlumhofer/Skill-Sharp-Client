namespace OpenSmi.Client.Tests;

[Collection("server")]
public class RemoteServerTests
{
    [Fact]
    public async Task Connect_reports_connected_after_connecting_and_browsing()
    {
        var server = new RemoteServer(ServerFixture.Options());
        var seen = new List<RemoteServerStatus>();
        server.StatusChanged += (_, e) => { lock (seen) seen.Add(e.Status); return Task.CompletedTask; };

        await using (server)
        {
            await server.ConnectAsync();
            Assert.True(server.IsConnected);
            Assert.Equal(RemoteServerStatus.Connected, server.Status);
        }

        Assert.Equal(
            [RemoteServerStatus.Connecting, RemoteServerStatus.Browsing, RemoteServerStatus.Connected,
             RemoteServerStatus.Disconnecting, RemoteServerStatus.Disconnected],
            seen);
    }

    [Fact]
    public async Task Concurrent_disconnects_tear_down_once_and_the_instance_reconnects()
    {
        var server = await ServerFixture.ConnectAsync(rules: []);
        var seen = new List<RemoteServerStatus>();
        server.StatusChanged += async (_, e) =>
        {
            lock (seen) seen.Add(e.Status);
            if (e.Status == RemoteServerStatus.Disconnecting)
            {
                await server.DisconnectAsync(); // re-entrant call must not wait for itself
                await Task.Delay(200); // keeps the teardown open while the other callers arrive
                lock (seen) seen.Add(RemoteServerStatus.Disconnecting); // marks the handler's end: no second teardown may overtake it
            }
        };

        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = Enumerable.Range(0, 5).Select(_ => Task.Run(async () =>
        {
            await start.Task;
            await server.DisconnectAsync();
            return server.Status;
        })).ToList();
        start.SetResult();

        Assert.All(await Task.WhenAll(calls).WaitAsync(TimeSpan.FromSeconds(30)), s => Assert.Equal(RemoteServerStatus.Disconnected, s));
        Assert.Equal([RemoteServerStatus.Disconnecting, RemoteServerStatus.Disconnecting, RemoteServerStatus.Disconnected], seen);

        await server.ConnectAsync();
        Assert.Equal(RemoteServerStatus.Connected, server.Status);
        await server.DisconnectAsync().WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(RemoteServerStatus.Disconnected, server.Status);
    }

    [Fact]
    public async Task Wrong_password_fails_without_retry()
    {
        var server = new RemoteServer(ServerFixture.Options() with { Password = "wrong" });
        await Assert.ThrowsAsync<OpenSmiAuthenticationException>(() => server.ConnectAsync());
        Assert.Equal(RemoteServerStatus.Disconnected, server.Status);
    }

    [Fact]
    public async Task Unreachable_server_fails_after_max_retries()
    {
        var server = new RemoteServer(ServerFixture.Options() with { Url = "opc.tcp://localhost:4999/server" });
        await Assert.ThrowsAsync<OpenSmiConnectionException>(() => server.ConnectAsync(new ConnectRetryOptions(MaxRetries: 2, MinDelaySeconds: 0.1)));
    }

    [Fact]
    public async Task Namespace_index_comes_from_the_namespace_array()
    {
        await using var server = await ServerFixture.ConnectAsync(rules: []);
        Assert.Equal(4, server.GetNamespaceIndex("http://opcfoundation.org/UA/Machinery/"));
        Assert.Throws<KeyNotFoundException>(() => server.GetNamespaceIndex("urn:nope"));
    }

    [Fact]
    public async Task Watchdog_interval_sets_the_keep_alive_period_default_one_second()
    {
        await using (var byDefault = await ServerFixture.ConnectAsync(rules: []))
            Assert.Equal(1000, byDefault.Session!.KeepAliveInterval);
        await using var custom = await new RemoteServer(ServerFixture.Options(rules: []) with
            { WatchdogInterval = TimeSpan.FromMilliseconds(500), AutoReconnect = false }).ConnectAsync();
        Assert.Equal(500, custom.Session!.KeepAliveInterval);
    }

    [Fact]
    public async Task Time_updated_fires_as_heartbeat()
    {
        await using var server = await ServerFixture.ConnectAsync(rules: []);
        var tick = new TaskCompletionSource<DateTime>();
        server.TimeUpdated += (_, e) => { tick.TrySetResult(e.Time); return Task.CompletedTask; };
        var time = await tick.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(Math.Abs((DateTime.UtcNow - time).TotalSeconds) < 30);
    }
}
