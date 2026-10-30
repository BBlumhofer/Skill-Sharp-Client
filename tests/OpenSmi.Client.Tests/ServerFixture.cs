[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace OpenSmi.Client.Tests;

/// <summary>
/// The shared dummy server for the "server" collection on port <c>OPENSMI_TEST_PORT</c> (default 4843).
/// A server already listening there is reused; otherwise a private <see cref="TestServer"/> is started.
/// </summary>
public sealed class ServerFixture : IAsyncLifetime
{
    public static readonly int Port = int.TryParse(Environment.GetEnvironmentVariable("OPENSMI_TEST_PORT"), out var p) ? p : 4843;
    public static string Url => $"opc.tcp://localhost:{Port}/server";
    private TestServer? _server;

    public async Task InitializeAsync()
    {
        if (!TestServer.IsListening(Port)) _server = await TestServer.StartAsync(Port);
    }

    public async Task DisposeAsync()
    {
        if (_server != null) await _server.DisposeAsync();
    }

    public static RemoteServerOptions Options(string user = "orchestrator", IReadOnlyList<BrowseRule>? rules = null) =>
        new() { Url = Url, Username = user, Password = user, BrowseRules = rules };

    public static async Task<RemoteServer> ConnectAsync(string user = "orchestrator", IReadOnlyList<BrowseRule>? rules = null)
    {
        await WaitForClosedSessionsAsync();
        return await new RemoteServer(Options(user, rules)).ConnectAsync();
    }

    /// <summary>
    /// opensmi-server notices closed sessions only in a 1 s loop and then releases the lock of that session's
    /// *user*, even one the same user has taken again meanwhile (access_control._check_for_closed_sessions_loop,
    /// lock._on_session_closed). Waiting before each connect keeps the previous test's session out of the next one.
    /// </summary>
    public static Task WaitForClosedSessionsAsync() => Task.Delay(TimeSpan.FromSeconds(1.5));
}

[CollectionDefinition("server")]
public sealed class ServerCollection : ICollectionFixture<ServerFixture>;
