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

        static Task OnNewMessage(object sender, NotificationReceivedEventArgs e)
        {
            Console.WriteLine($"new message: {((BaseRemoteComponent)sender).Name} {e.Notification}");
            return Task.CompletedTask;
        }

        // subscribe to events with async handlers:
        machine.NotificationReceived += OnNewMessage;
        machine.StatusChanged += (sender, e) =>
        {
            Console.WriteLine($"new status: {((BaseRemoteComponent)sender).Name} '{e.State}' !");
            return Task.CompletedTask;
        };
        machine.Components["DummyComponent"].NotificationReceived += OnNewMessage;

        await Task.Delay(duration ?? TimeSpan.FromSeconds(120), cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}
