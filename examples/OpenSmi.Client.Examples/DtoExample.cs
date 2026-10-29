namespace OpenSmi.Client.Examples;

/// <summary>Port of examples/dto.py: print every machine as a DTO in JSON.</summary>
public static class DtoExample
{
    public static async Task RunAsync(string url, CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new() { Url = url, Username = "visitor", Password = "visitor" })
            .ConnectAsync(cancellationToken: cancellationToken);
        foreach (var machine in server.Machines.Values)
            Console.WriteLine(Print.Json(machine.ToDto()));
    }
}
