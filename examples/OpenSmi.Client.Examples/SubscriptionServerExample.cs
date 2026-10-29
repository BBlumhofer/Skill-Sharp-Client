namespace OpenSmi.Client.Examples;

/// <summary>Port of examples/subscription_server.py: print status changes and the server time until cancelled (Ctrl+C) or <c>duration</c> is over.</summary>
public static class SubscriptionServerExample
{
    public static async Task RunAsync(string url, TimeSpan? duration = null, CancellationToken cancellationToken = default)
    {
        var timeUpdateCount = 0;

        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new() { Url = url, Username = "visitor", Password = "visitor" })
            .ConnectAsync(cancellationToken: cancellationToken);
        server.StatusChanged += (_, e) =>
        {
            Console.WriteLine($"new status: {e.Status}");
            return Task.CompletedTask;
        };
        server.TimeUpdated += (_, e) =>
        {
            Console.WriteLine($"({timeUpdateCount++:000}) received server time: {e.Time:O}");
            return Task.CompletedTask;
        };

        Console.WriteLine("Press Ctrl+C to exit!");
        await Task.Delay(duration ?? Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}
