namespace OpenSmi.Client.Examples;

/// <summary>Port of examples/history.py: read the last 10 historized values of a monitoring variable.</summary>
public static class HistoryExample
{
    public static async Task RunAsync(string url, CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new() { Url = url, Username = "visitor", Password = "visitor" })
            .ConnectAsync(cancellationToken: cancellationToken);
        var machine = server.Machines["DummyMachine"];

        var variable = machine.Components["DummyComponent"].Monitoring["RandomValue"];
        Console.WriteLine(variable);

        var history = await variable.ReadHistoryAsync(numValues: 10, cancellationToken: cancellationToken); // limit to the last 10
        foreach (var entry in history) Console.WriteLine(entry);
    }
}
