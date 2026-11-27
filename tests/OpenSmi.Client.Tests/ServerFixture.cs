using System.Reflection;
using Xunit.Sdk;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
[assembly: OpenSmi.Client.Tests.SettleSessions]

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

    /// <summary>
    /// opensmi-server notices closed sessions only once per second and then releases the lock of that session's
    /// <em>user</em>, even if a newer session of the same user holds it now. Waiting this long after closing a
    /// session keeps one test's late release from hitting the next test.
    /// </summary>
    public static Task SettleAsync() => Task.Delay(TimeSpan.FromSeconds(1.2));

    public static async Task<RemoteServer> ConnectAsync(string user = "orchestrator", IReadOnlyList<BrowseRule>? rules = null) =>
        await new RemoteServer(Options(user, rules)).ConnectAsync();
}

[CollectionDefinition("server")]
public sealed class ServerCollection : ICollectionFixture<ServerFixture>;

/// <summary>Runs <see cref="ServerFixture.SettleAsync"/> after every test of a class with a server (collection or class fixture).</summary>
[AttributeUsage(AttributeTargets.Assembly)]
public sealed class SettleSessionsAttribute : BeforeAfterTestAttribute
{
    public override void After(MethodInfo methodUnderTest)
    {
        var type = methodUnderTest.DeclaringType;
        var usesServer = type?.GetCustomAttribute<CollectionAttribute>() != null
            || type?.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IClassFixture<>)) == true;
        if (usesServer)
            ServerFixture.SettleAsync().GetAwaiter().GetResult();
    }
}
