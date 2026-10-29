namespace OpenSmi.Client.Examples;

/// <summary>Port of examples/subscription_component.py: print notifications and status changes for 120 s.</summary>
public static class SubscriptionComponentExample
{
    public static async Task RunAsync(string url, TimeSpan? duration = null, CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new() { Url = url, Username = "visitor", Password = "visitor" })
            .ConnectAsync(cancellationToken: cancellationToken);
        var machine = server.Machines["DummyMachine"];

        static Task OnNewMessage((BaseRemoteComponent Sender, NotificationEvent Event) a, CancellationToken _)
        {
            Console.WriteLine($"new message: {a.Sender.Name} {a.Event}");
            return Task.CompletedTask;
        }

        // subscribe to events by connecting async functions to signals:
        machine.NotificationReceived.Connect(OnNewMessage);
        machine.StatusChanged.Connect((a, _) =>
        {
            Console.WriteLine($"new status: {a.Sender.Name} '{a.State}' !");
            return Task.CompletedTask;
        });
        machine.Components["DummyComponent"].NotificationReceived.Connect(OnNewMessage);

        await Task.Delay(duration ?? TimeSpan.FromSeconds(120), cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}
