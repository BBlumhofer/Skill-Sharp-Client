namespace OpenSmi.Client.Tests;

/// <summary>Port of opensmi-core 0.7 <c>paths.py</c>; expected values come from the Python original.</summary>
public class PathTests
{
    [Theory]
    [InlineData("/M1/Components/Gripper", "/M1/**/Grip*", true)]
    [InlineData("/M1/Components/Gripper", "/M1/*", false)]
    [InlineData("/M1", "/M?", true)]
    [InlineData("/M1", "/**", true)]
    [InlineData("/", "/**", true)]
    [InlineData("/a/b/c", "/a/**/c", true)]
    [InlineData("/a/c", "/a/**/c", true)]
    [InlineData("/a/[x]", "/a/[[]x]", true)]
    [InlineData("/a/b", "/a/[!a]", true)]
    [InlineData("/a/b", "/a/[a-c]", true)]
    [InlineData("/a/*", "/a/[*]", true)]
    [InlineData("/a/x", "/a/[*]", false)]
    public void Glob_matches_whole_paths(string path, string pattern, bool expected) =>
        Assert.Equal(expected, ObjectPath.Parse(path).Match(pattern));

    [Theory]
    [InlineData("M1/*")]
    [InlineData("/a//b")]
    [InlineData("/a/")]
    public void Invalid_paths_are_rejected(string text) => Assert.Throws<ArgumentException>(() => ObjectPath.Parse(text));

    [Fact]
    public void Paths_are_values_with_tree_order()
    {
        var gripper = ObjectPath.Parse("/M1/Components/Gripper");

        Assert.Equal(gripper, ObjectPath.Root / "M1" / "Components" / "Gripper");
        Assert.Equal("/M1/Components/Gripper", gripper.ToString());
        Assert.Equal("Gripper", gripper.Name);
        Assert.Equal(ObjectPath.Parse("/M1/Components"), gripper.Parent);
        Assert.Equal("/", ObjectPath.Root.ToString());
        Assert.Equal("", ObjectPath.Root.Name);
        Assert.Throws<InvalidOperationException>(() => ObjectPath.Root.Parent);
        Assert.Throws<ArgumentException>(() => ObjectPath.Root / "a/b");

        string[] sorted = ["/M1/SkillSet/Pick", "/M10", "/M1", "/M1/Components/Gripper/SkillSet/Grip",
            "/M1/Components/Gripper/Components/Finger/SkillSet/Grip"];
        Assert.Equal(
            ["/M1", "/M1/Components/Gripper/Components/Finger/SkillSet/Grip", "/M1/Components/Gripper/SkillSet/Grip",
                "/M1/SkillSet/Pick", "/M10"],
            sorted.Select(ObjectPath.Parse).Order().Select(p => p.ToString()));
    }

    [Fact]
    public void Relative_and_escape()
    {
        Assert.True(ObjectPath.Parse("/a/b").IsRelativeTo("/a"));
        Assert.False(ObjectPath.Parse("/ab").IsRelativeTo("/a"));
        Assert.True(ObjectPath.Parse("/a").IsRelativeTo(ObjectPath.Root));
        Assert.Equal("a[*]b[?][[]c]", ObjectPath.Escape("a*b?[c]"));
    }

    [Fact]
    public void Unique_segments_are_sanitized_and_suffixed_in_node_id_order()
    {
        var segments = PathSegments.Unique([
            (new("urn:b", "x"), "Temperature"),
            (new("urn:a", "raw.duplicate"), "Temperature"),
            (new("urn:b", "s"), "Pick/Place"),
            (new("urn:b", "e"), ""),
            (new("urn:b", 5u), "A"),
            (new("urn:b", 3u), "A"),
            (new("urn:b", "A_2"), "A_2"),
        ]);

        Assert.Equal("Temperature", segments[new("urn:a", "raw.duplicate")]);
        Assert.Equal("Temperature_2", segments[new("urn:b", "x")]);
        Assert.Equal("Pick_Place", segments[new("urn:b", "s")]);
        Assert.Equal("_", segments[new("urn:b", "e")]);
        Assert.Equal("A", segments[new("urn:b", 3u)]);
        Assert.Equal("A_3", segments[new("urn:b", 5u)]); // A_2 is another sibling's own name
        Assert.Equal("A_2", segments[new("urn:b", "A_2")]);
    }

    [Fact]
    public void Browse_matchers_work_on_paths()
    {
        var gripper = ObjectPath.Parse("/M1/Components/Gripper");

        Assert.True(Match.Path("/M1/Components/Gripper")(gripper));
        Assert.False(Match.Path(gripper)(gripper / "SkillSet"));
        Assert.True(Match.PathPrefix("/M1/Components")(gripper));
        Assert.False(Match.PathPrefix("/M1/Components")(ObjectPath.Parse("/M1/ComponentsX")));
        Assert.True(Match.Name("Gripper")(gripper));
        Assert.True(Match.Glob("/M1/**/Grip*")(gripper));
        Assert.False(Match.Glob("/M1/*")(gripper));
        Assert.Throws<ArgumentException>(() => Match.Path("M1/*"));
        Assert.Throws<ArgumentException>(() => Match.PathPrefix("M1/*"));
        Assert.Throws<ArgumentException>(() => Match.Glob("M1/*"));
    }

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
            BrowseRules.Evaluate(rules, ObjectPath.Parse("/DummyMachine")));
        Assert.Equal(BrowseFeature.Machines | BrowseFeature.Monitoring,
            BrowseRules.Evaluate(rules, ObjectPath.Parse("/DummyMachine/Components/DummyComponent")));
        Assert.Equal(BrowseFeature.None,
            BrowseRules.Evaluate([BrowseRules.When(Match.Path("/X"), BrowseFeature.Lock)], ObjectPath.Parse("/Y")));
    }

    [Fact]
    public void Combinators_compose()
    {
        var m = Match.AllOf(Match.PathPrefix("/M"), Match.Not(Match.Name("Lock")));
        Assert.True(m(ObjectPath.Parse("/M/C")));
        Assert.False(m(ObjectPath.Parse("/M/Lock")));
        Assert.True(Match.Any(Match.Path("/A"), Match.Path("/B"))(ObjectPath.Parse("/B")));
    }
}
