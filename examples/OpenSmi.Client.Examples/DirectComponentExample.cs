using Opc.Ua;

namespace OpenSmi.Client.Examples;

/// <summary>Port of examples/direct_component.py: browse nothing on connect, then build one component from its NodeId.</summary>
public static class DirectComponentExample
{
    public static async Task RunAsync(string url, CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new()
        {
            Url = url, Username = "visitor", Password = "visitor",
            BrowseRules = [], // browse nothing automatically
        }).ConnectAsync(cancellationToken: cancellationToken);

        var component = await RemoteComponent.CreateAsync(server, "DummyComponent",
            NodeId.Parse("ns=1;s=DummyMachine.Components.DummyComponent"),
            browseRules: [BrowseRule.Default], // browse everything, can be specified as usual
            cancellationToken: cancellationToken);

        Console.WriteLine(Print.Json(component.ToDto()));
    }
}
