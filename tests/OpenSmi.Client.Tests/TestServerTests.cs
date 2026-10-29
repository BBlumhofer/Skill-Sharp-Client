namespace OpenSmi.Client.Tests;

public class TestServerTests
{
    [Fact]
    public async Task Dedicated_server_runs_on_its_own_port_and_restarts()
    {
        await using var testServer = await TestServer.StartAsync(port: 4851);
        await using (var client = await new RemoteServer(testServer.Options() with { BrowseRules = [] }).ConnectAsync())
            Assert.True(client.IsConnected);

        await testServer.StopAsync();
        await Assert.ThrowsAsync<OpenSmiConnectionException>(() =>
            new RemoteServer(testServer.Options()).ConnectAsync(new ConnectRetryOptions(MaxRetries: 1)));

        await testServer.StartAgainAsync();
        await using var again = await new RemoteServer(testServer.Options() with { BrowseRules = [] }).ConnectAsync();
        Assert.True(again.IsConnected);
    }
}
