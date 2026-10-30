namespace OpenSmi.Client.Tests;

/// <summary>Deviations from SPEC §10 that no other test covers (docs/opensmi/DEVIATIONS.md).</summary>
[Collection("server")]
public class DeviationTests
{
    [Fact]
    public async Task Skill_wait_ends_with_timeout_or_cancellation_instead_of_hanging()
    {
        await using var server = await ServerFixture.ConnectAsync(rules: [BrowseRules.When(Match.All, BrowseFeature.Machines, BrowseFeature.SkillSet)]);
        var skill = server.Machines["DummyMachine"].SkillSet["ContinuousSkill"]; // never reaches Completed

        await Assert.ThrowsAsync<TimeoutException>(() => skill.WaitForStateAsync(SkillState.Completed, TimeSpan.FromMilliseconds(300)));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => skill.WaitForStateAsync(SkillState.Completed, cancellationToken: cts.Token));
    }
}
