namespace OpenSmi.Client.Examples;

/// <summary>
/// Port of examples/example.py: lock the machine, bring the StartupSkill up from a cold start if required
/// and then run DummySkill once.
/// </summary>
public static class ColdStartExample
{
    public static async Task RunAsync(string url, CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new() { Url = url, Username = "operator", Password = "operator" })
            .ConnectAsync(cancellationToken: cancellationToken);
        var machine = server.Machines["DummyMachine"];

        foreach (var (name, s) in machine.SkillSet) // iterate through available skills as demonstration
        {
            Console.WriteLine($"Skill '{name}' is in state '{s.CurrentState}'");
            s.StateChanged += (sender, e) => // get notified of skill state changes
            {
                Console.WriteLine($"Skill `{((RemoteSkill)sender).Name}` now in state `{e.State}`");
                return Task.CompletedTask;
            };
        }

        // Step 1: acquire the lock of the machine
        await machine.Lock.InitLockAsync(cancellationToken);

        // Step 2: reset & then start the StartupSkill if required
        // Read the state from the server: right after connecting, the cached CurrentState may not be filled yet.
        var startup = machine.SkillSet["StartupSkill"];
        var state = await startup.ReadCurrentStateAsync(cancellationToken);
        if (state != SkillState.Running)
        {
            if (state != SkillState.Ready) await startup.ResetAsync(wait: true, cancellationToken: cancellationToken);
            await startup.StartAsync(StartWait.RunningOrCompleted, cancellationToken: cancellationToken);
        }

        // Step 3: the machine should be ready now, use it :)
        var skill = machine.SkillSet["DummySkill"];
        Console.WriteLine($"parameters before: {Print.Values(await skill.ParameterSet.ReadAllAsync(cancellationToken))}");
        await skill.ParameterSet.WriteAllAsync(new Dictionary<string, object?> { ["x"] = 5, ["y"] = 42 }, cancellationToken);
        await skill.StartAsync(StartWait.Completion, cancellationToken: cancellationToken);
        Console.WriteLine($"results: {Print.Values(await skill.FinalResultData.ReadAllAsync(cancellationToken))}");
        await skill.ResetAsync(wait: true, cancellationToken: cancellationToken); // reset after getting the results!
    }
}
