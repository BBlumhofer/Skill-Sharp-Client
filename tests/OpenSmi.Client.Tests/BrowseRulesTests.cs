namespace OpenSmi.Client.Tests;

public class BrowseRulesTests
{
    [Theory]
    [InlineData("/DummyMachine", true)]
    [InlineData("/DummyMachine/X", false)]
    [InlineData("/Other/DummyMachine", true)]
    public void Name_matches_the_last_segment(string path, bool expected) =>
        Assert.Equal(expected, Match.Name("DummyMachine")(path));

    [Theory]
    [InlineData("/A", true)]
    [InlineData("/A/B", true)]
    [InlineData("/AB", false)]
    public void PathPrefix_matches_path_and_descendants(string path, bool expected) =>
        Assert.Equal(expected, Match.PathPrefix("/A")(path));

    [Fact]
    public void Evaluate_ors_the_features_of_all_matching_rules()
    {
        // Rules of opensmi-client/examples/browse_rules.py
        BrowseRule[] rules =
        [
            BrowseRules.When(Match.All, BrowseFeature.Machines, BrowseFeature.Monitoring),
            BrowseRules.When(Match.Name("DummyMachine"), BrowseFeature.Components, BrowseFeature.SkillSet),
        ];

        Assert.Equal(BrowseFeature.Machines | BrowseFeature.Monitoring | BrowseFeature.Components | BrowseFeature.SkillSet,
            BrowseRules.Evaluate(rules, "/DummyMachine"));
        Assert.Equal(BrowseFeature.Machines | BrowseFeature.Monitoring, BrowseRules.Evaluate(rules, "/DummyMachine/DummyComponent"));
        Assert.Equal(BrowseFeature.None, BrowseRules.Evaluate([BrowseRules.When(Match.Path("/X"), BrowseFeature.Lock)], "/Y"));
    }

    [Fact]
    public void Combinators_compose()
    {
        var m = Match.AllOf(Match.PathPrefix("/M"), Match.Not(Match.Name("Lock")));
        Assert.True(m("/M/C"));
        Assert.False(m("/M/Lock"));
        Assert.True(Match.Any(Match.Path("/A"), Match.Path("/B"))("/B"));
    }
}
