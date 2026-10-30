using System.Diagnostics;
using System.Net.Sockets;
using System.Text.RegularExpressions;

namespace OpenSmi.Client.Tests;

/// <summary>
/// A private opensmi-server dummy machine: a copy of <c>opensmi-server/examples/dummy</c> in a temp dir with
/// its own port (and optional encryption), so tests can stop/restart it and parallel runs do not share locks.
/// </summary>
public sealed partial class TestServer : IAsyncDisposable
{
    private readonly string _dir;
    private Process? _process;

    private TestServer(int port, string dir)
    {
        Port = port;
        _dir = dir;
    }

    public int Port { get; }
    public string Url => $"opc.tcp://localhost:{Port}/server";
    /// <summary>The server's own certificate (DER), present once an encrypted server has started.</summary>
    public string CertificatePath => Path.Combine(_dir, "server_certificate.der");

    public RemoteServerOptions Options(string user = "orchestrator") =>
        new() { Url = Url, Username = user, Password = user };

    /// <param name="customTypes">Adds the custom enum DataType <c>TestColor</c> (Red, Green, Blue) as
    /// <c>DummyComponent.ParameterSet.DummyEnumParameter</c> (initially Green, writable without lock).</param>
    public static async Task<TestServer> StartAsync(int port, bool encryption = false, bool customTypes = false)
    {
        var dir = Directory.CreateTempSubdirectory("opensmi-server-").FullName;
        foreach (var file in Directory.GetFiles(SourceDir()))
            File.Copy(file, Path.Combine(dir, Path.GetFileName(file)));
        var config = Path.Combine(dir, "config.toml");
        var toml = File.ReadAllText(config);
        toml = EndpointPort().Replace(toml, $":{port}/server\"");
        toml = Encryption().Replace(toml, $"encryption = {(encryption ? "true" : "false")}");
        File.WriteAllText(config, toml);
        if (customTypes) AddCustomTypes(dir);

        var server = new TestServer(port, dir);
        await server.StartAgainAsync();
        return server;
    }

    private static void AddCustomTypes(string dir)
    {
        File.WriteAllText(Path.Combine(dir, "custom_types.py"), """
            from enum import IntEnum


            class TestColor(IntEnum):
                Red = 0
                Green = 1
                Blue = 2

            """);
        var component = Path.Combine(dir, "dummy_component.py");
        var source = File.ReadAllText(component);
        const string anchor = "    DummyIntListParameter = UaVariable(initial_value=[42, 84])";
        if (!source.Contains(anchor)) throw new InvalidOperationException("dummy_component.py changed, cannot add custom types");
        source = "from custom_types import TestColor" + Environment.NewLine + source.Replace(anchor,
            anchor + Environment.NewLine + "    DummyEnumParameter = UaVariable(initial_value=TestColor.Green, bypass_lock=True)");
        File.WriteAllText(component, source);
    }

    public async Task StartAgainAsync()
    {
        if (IsListening(Port)) throw new InvalidOperationException($"Port {Port} is already in use");
        _process = Process.Start(new ProcessStartInfo("python", "main.py")
        {
            WorkingDirectory = _dir,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        }) ?? throw new InvalidOperationException("python could not be started");
        _process.BeginOutputReadLine();
        _process.BeginErrorReadLine();
        await WaitForPortAsync(Port, _process);
    }

    public async Task StopAsync()
    {
        if (_process is { HasExited: false })
        {
            _process.Kill(entireProcessTree: true);
            await _process.WaitForExitAsync();
        }
        _process?.Dispose();
        _process = null;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        try { Directory.Delete(_dir, recursive: true); }
        catch (IOException) { } // python may still hold __pycache__ for a moment; the temp dir is harmless
    }

    internal static async Task WaitForPortAsync(int port, Process process)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!IsListening(port))
        {
            if (process.HasExited || DateTime.UtcNow > deadline)
                throw new InvalidOperationException($"opensmi-server on port {port} did not start");
            await Task.Delay(200);
        }
        await Task.Delay(1000); // the server opens the port before the address space is complete
    }

    internal static bool IsListening(int port)
    {
        using var client = new TcpClient();
        try { client.Connect("localhost", port); return true; }
        catch (SocketException) { return false; }
    }

    internal static string SourceDir()
    {
        var env = Environment.GetEnvironmentVariable("OPENSMI_SERVER_DIR");
        if (env != null) return env;
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "OpenSmi.Client.slnx"))) dir = dir.Parent;
        return Path.Combine(dir?.FullName ?? throw new InvalidOperationException("repo root not found"),
            "..", "opensmi-server", "examples", "dummy");
    }

    [GeneratedRegex(@":\d+/server""")]
    private static partial Regex EndpointPort();

    [GeneratedRegex(@"encryption = (true|false)")]
    private static partial Regex Encryption();
}
