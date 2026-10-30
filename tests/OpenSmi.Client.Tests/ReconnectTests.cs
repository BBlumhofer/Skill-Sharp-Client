namespace OpenSmi.Client.Tests;

public class ReconnectTests
{
    private static async Task WaitForStatusAsync(RemoteServer server, RemoteServerStatus status, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (server.Status != status)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException($"Status stayed {server.Status}, expected {status}");
            await Task.Delay(100);
        }
    }

    private static Task<object?> NextValueAsync(RemoteVariable variable)
    {
        var next = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        variable.ValueChanged += (_, e) => { next.TrySetResult(e.Value.Value); return Task.CompletedTask; };
        return next.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Server_restart_reconnects_with_the_same_objects_and_notifications_resume()
    {
        await using var testServer = await TestServer.StartAsync(port: 4894);
        await using var client = await new RemoteServer(testServer.Options()).ConnectAsync();
        var machine = client.Machines["DummyMachine"];

        await testServer.StopAsync();
        await WaitForStatusAsync(client, RemoteServerStatus.Reconnecting, TimeSpan.FromSeconds(30));
        await testServer.StartAgainAsync();
        await WaitForStatusAsync(client, RemoteServerStatus.Connected, TimeSpan.FromSeconds(30));

        Assert.Same(machine, client.Machines["DummyMachine"]);
        await NextValueAsync(machine.Monitoring["RandomValue"]);
    }

    [Fact]
    public async Task Disconnect_while_reconnecting_ends_disconnected()
    {
        await using var testServer = await TestServer.StartAsync(port: 4895);
        var client = await new RemoteServer(testServer.Options() with { BrowseRules = [] }).ConnectAsync();
        await testServer.StopAsync();
        await WaitForStatusAsync(client, RemoteServerStatus.Reconnecting, TimeSpan.FromSeconds(30));

        await client.DisconnectAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(RemoteServerStatus.Disconnected, client.Status);
        Assert.Null(client.Session);
        await testServer.StartAgainAsync();
        await Task.Delay(3000); // a reconnect left running would flip the status back
        Assert.Equal(RemoteServerStatus.Disconnected, client.Status);
    }

    [Fact]
    public async Task Without_auto_reconnect_a_dead_server_ends_disconnected()
    {
        await using var testServer = await TestServer.StartAsync(port: 4922);
        var client = await new RemoteServer(testServer.Options() with { AutoReconnect = false, BrowseRules = [] }).ConnectAsync();
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.StatusChanged += (_, e) =>
        {
            if (e.Status == RemoteServerStatus.Disconnected) disconnected.TrySetResult();
            return Task.CompletedTask;
        };

        await testServer.StopAsync();

        await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(RemoteServerStatus.Disconnected, client.Status);
        Assert.Null(client.Session);
    }

    [Fact]
    public async Task Short_network_drop_reactivates_the_same_session()
    {
        await using var testServer = await TestServer.StartAsync(port: 4923);
        using var proxy = new TcpProxy(port: 4924, target: 4923);
        await using var client = await new RemoteServer(testServer.Options() with { Url = "opc.tcp://localhost:4924/server" })
            .ConnectAsync();
        var session = client.Session;
        var machine = client.Machines["DummyMachine"];

        proxy.Cut();
        await WaitForStatusAsync(client, RemoteServerStatus.Reconnecting, TimeSpan.FromSeconds(30));
        proxy.Restore();
        await WaitForStatusAsync(client, RemoteServerStatus.Connected, TimeSpan.FromSeconds(30));

        Assert.Same(session, client.Session); // reactivated, not recreated: the server kept the session
        Assert.Same(machine, client.Machines["DummyMachine"]);
        await NextValueAsync(machine.Monitoring["RandomValue"]);
    }

    [Fact]
    public async Task Stale_heartbeat_rebuilds_the_subscription_with_all_items()
    {
        await using var testServer = await TestServer.StartAsync(port: 4896);
        await using var client = await new RemoteServer(testServer.Options() with { HeartbeatStaleAfter = TimeSpan.FromSeconds(2) })
            .ConnectAsync();
        var session = client.Session!;
        var before = session.Subscriptions.Single();
        var itemCount = before.MonitoredItemCount;

        // Kill the subscription behind the client's back: the session stays alive, notifications stop.
        await session.DeleteSubscriptionsAsync(null, [before.Id], CancellationToken.None);

        await NextValueAsync(client.Machines["DummyMachine"].Monitoring["RandomValue"]);
        var after = session.Subscriptions.Single();
        Assert.NotEqual(before.Id, after.Id);
        Assert.Equal(itemCount, after.MonitoredItemCount); // data and event items
        Assert.Equal(RemoteServerStatus.Connected, client.Status);
    }
}
