using System.Security.Cryptography.X509Certificates;
using Opc.Ua;

namespace OpenSmi.Client.Tests;

/// <summary>One encrypted dummy server (no NoSecurity endpoint) shared by the security tests.</summary>
public sealed class EncryptedServerFixture : IAsyncLifetime
{
    public TestServer Server { get; private set; } = null!;
    public async Task InitializeAsync() => Server = await TestServer.StartAsync(4862, encryption: true);
    public async Task DisposeAsync() => await Server.DisposeAsync();
}

public class SecurityTests(EncryptedServerFixture fixture) : IClassFixture<EncryptedServerFixture>
{
    private static string TempStore() => Directory.CreateTempSubdirectory("opensmi-pki-").FullName;

    private RemoteServerOptions Options(SecurityOptions security) =>
        fixture.Server.Options() with { Security = security };

    private static SecurityOptions Encrypted(string store, bool autoAccept = false, string? trusted = null) =>
        new(SecurityPolicies.Basic256Sha256, MessageSecurityMode.SignAndEncrypt, store, autoAccept, trusted);

    [Fact]
    public async Task Encrypted_connect_with_auto_accept_browses_the_machine()
    {
        await using var server = await new RemoteServer(Options(Encrypted(TempStore(), autoAccept: true))).ConnectAsync();
        Assert.Equal(MessageSecurityMode.SignAndEncrypt, server.Session!.Endpoint.SecurityMode);
        Assert.Contains("DummyMachine", server.Machines.Keys);
    }

    [Fact]
    public async Task Client_certificate_is_created_once_and_reused()
    {
        var store = TempStore();
        var options = Options(Encrypted(store, autoAccept: true)) with { BrowseRules = [] };
        await using (await new RemoteServer(options).ConnectAsync()) { }
        var first = ClientThumbprint(store);
        await using (await new RemoteServer(options).ConnectAsync()) { }
        Assert.Equal(first, ClientThumbprint(store));
    }

    [Fact]
    public async Task Untrusted_server_certificate_fails_without_retry()
    {
        var server = new RemoteServer(Options(Encrypted(TempStore())) with { BrowseRules = [] });
        var started = DateTime.UtcNow;
        await Assert.ThrowsAsync<OpenSmiConnectionException>(() =>
            server.ConnectAsync(new ConnectRetryOptions(MaxRetries: null, MinDelaySeconds: 5)));
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(5), "a rejected certificate must not be retried");
        Assert.Equal(RemoteServerStatus.Disconnected, server.Status);
    }

    [Fact]
    public async Task Server_certificate_in_trust_store_connects_without_auto_accept()
    {
        var trusted = Path.Combine(TempStore(), "certs");
        Directory.CreateDirectory(trusted);
        File.Copy(fixture.Server.CertificatePath, Path.Combine(trusted, "server.der"));

        await using var server = await new RemoteServer(
            Options(Encrypted(TempStore(), trusted: Path.GetDirectoryName(trusted))) with { BrowseRules = [] }).ConnectAsync();
        Assert.True(server.IsConnected);
    }

    [Fact]
    public async Task No_security_against_an_encrypted_server_fails()
    {
        var server = new RemoteServer(Options(SecurityOptions.None) with { BrowseRules = [] });
        await Assert.ThrowsAsync<OpenSmiConnectionException>(() => server.ConnectAsync());
    }

    private static string ClientThumbprint(string store)
    {
        var cert = X509CertificateLoader.LoadCertificateFromFile(
            Assert.Single(Directory.GetFiles(Path.Combine(store, "own", "certs"))));
        Assert.Equal("OpenSMI C# Client", cert.GetNameInfo(X509NameType.SimpleName, false));
        return cert.Thumbprint;
    }
}
