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
    public async Task Time_updated_fires_as_heartbeat()
    {
        await using var server = await ServerFixture.ConnectAsync(rules: []);
        var tick = new TaskCompletionSource<DateTime>();
        server.TimeUpdated += (_, e) => { tick.TrySetResult(e.Time); return Task.CompletedTask; };
        var time = await tick.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(Math.Abs((DateTime.UtcNow - time).TotalSeconds) < 30);
    }
}
