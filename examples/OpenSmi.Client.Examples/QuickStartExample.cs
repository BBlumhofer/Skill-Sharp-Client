namespace OpenSmi.Client.Examples;

/// <summary>
/// Port of examples/quick_start.py: lock the machine and run a skill once. The defaults fit the
/// opensmi-server quick_start example (opc.tcp://localhost:4841/).
/// </summary>
public static class QuickStartExample
{
    public static async Task RunAsync(string url, string machineName = "ExampleMachine", string skillName = "ExampleSkill",
        CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new() { Url = url, Username = "operator", Password = "operator" })
            .ConnectAsync(cancellationToken: cancellationToken);
        var machine = server.Machines[machineName];

        await machine.Lock.InitLockAsync(cancellationToken);

        var skill = machine.SkillSet[skillName];
        // reset the skill if necessary
        if (await skill.ReadCurrentStateAsync(cancellationToken) != SkillState.Ready)
            await skill.ResetAsync(wait: true, cancellationToken: cancellationToken);

        // bulk write the parameters
        await skill.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["x"] = 5, ["y"] = 10 }, cancellationToken);

        // start the skill and wait for completion
        await skill.StartAsync(StartWait.Completion, cancellationToken: cancellationToken);

        // retrieve the results before resetting
        Console.WriteLine($"results: {Print.Values(await skill.FinalResultData.ReadAllAsync(cancellationToken))}");

        await skill.ResetAsync(wait: true, cancellationToken: cancellationToken);
    }
}
