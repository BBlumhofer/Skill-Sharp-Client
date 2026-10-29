namespace OpenSmi.Client.Examples;

/// <summary>Port of examples/auto_lock.py: keep the machine locked until cancelled (Ctrl+C) or <c>duration</c> is over.</summary>
public static class AutoLockExample
{
    public static async Task RunAsync(string url, TimeSpan? duration = null, CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new() { Url = url, Username = "orchestrator", Password = "orchestrator" })
            .ConnectAsync(cancellationToken: cancellationToken);
        var machine = server.Machines["DummyMachine"];
        await using var autoLock = new AutoLock(machine.Lock);
        await autoLock.LockAsync(cancellationToken: cancellationToken);
        Console.WriteLine($"{machine.Name} locked by {(await machine.Lock.ReadStateAsync(cancellationToken: cancellationToken)).LockingUser}");

        Console.WriteLine("Press Ctrl+C to exit!");
        await Task.Delay(duration ?? Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}
