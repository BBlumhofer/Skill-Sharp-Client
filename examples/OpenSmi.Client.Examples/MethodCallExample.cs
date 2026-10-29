namespace OpenSmi.Client.Examples;

/// <summary>Port of examples/method_call.py: lock the machine and call DummyMethod.</summary>
public static class MethodCallExample
{
    public static async Task RunAsync(string url, CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new() { Url = url, Username = "operator", Password = "operator" })
            .ConnectAsync(cancellationToken: cancellationToken);
        var machine = server.Machines["DummyMachine"];

        // Step 1: lock the machine
        await machine.Lock.InitLockAsync(cancellationToken);

        // Step 2: profit!
        var method = machine.MethodSet["DummyMethod"];
        Console.WriteLine($"parameters before: {Print.Values(await method.ParameterSet.ReadAllAsync(cancellationToken))}");
        await method.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["a"] = 5, ["b"] = 42 }, cancellationToken);
        await method.CallAsync(cancellationToken);
        Console.WriteLine($"results: {Print.Values(await method.FinalResultData.ReadAllAsync(cancellationToken))}");
    }
}
