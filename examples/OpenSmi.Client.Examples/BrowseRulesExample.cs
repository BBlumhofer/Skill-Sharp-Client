namespace OpenSmi.Client.Examples;

/// <summary>Port of examples/browse_rules.py: browse only what the rules ask for and print the result.</summary>
public static class BrowseRulesExample
{
    public static async Task RunAsync(string url, CancellationToken cancellationToken = default)
    {
        BrowseRule[] browseRules =
        [
            BrowseRules.When(
                Match.All, // matches everything
                BrowseFeature.Machines,
                BrowseFeature.Monitoring),
                // BrowseFeature.Identification
            BrowseRules.When(
                Match.Name("DummyMachine"),
                BrowseFeature.Components,
                BrowseFeature.SkillSet),
        ];

        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new()
        {
            Url = url, Username = "visitor", Password = "visitor", BrowseRules = browseRules,
        }).ConnectAsync(cancellationToken: cancellationToken);
        foreach (var machine in server.Machines.Values)
            Console.WriteLine(Print.Json(machine.ToDto()));
    }
}
