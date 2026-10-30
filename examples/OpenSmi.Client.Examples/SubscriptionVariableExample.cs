namespace OpenSmi.Client.Examples;

/// <summary>Port of examples/subscription_variable.py: print value changes of DummyComponent's monitoring for 120 s.</summary>
public static class SubscriptionVariableExample
{
    public static async Task RunAsync(string url, TimeSpan? duration = null, CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new() { Url = url, Username = "visitor", Password = "visitor" })
            .ConnectAsync(cancellationToken: cancellationToken);
        var machine = server.Machines["DummyMachine"];

        foreach (var variable in machine.Components["DummyComponent"].Monitoring.Values)
            variable.ValueChanged += (sender, e) =>
            {
                Console.WriteLine($"new value: {((RemoteVariable)sender).Path} = {e.Value.Value}");
                return Task.CompletedTask;
            };

        await Task.Delay(duration ?? TimeSpan.FromSeconds(120), cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}
