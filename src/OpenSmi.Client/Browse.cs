namespace OpenSmi.Client;

/// <summary>Port of <c>client/browse.py</c>: which features to browse for an object, decided by its path.</summary>
public sealed record BrowseRule(Func<ObjectPath, bool> Match, BrowseFeature Features)
{
    public static BrowseRule Default { get; } = new(OpenSmi.Client.Match.All, BrowseFeature.All);
}

public static class BrowseRules
{
    public static BrowseRule When(Func<ObjectPath, bool> match, params BrowseFeature[] features) =>
        new(match, features.Aggregate(BrowseFeature.None, (a, f) => a | f));

    /// <summary>All matching rules contribute their features.</summary>
    public static BrowseFeature Evaluate(IEnumerable<BrowseRule> rules, ObjectPath path) =>
        rules.Where(r => r.Match(path)).Aggregate(BrowseFeature.None, (a, r) => a | r.Features);
}

/// <summary>Matchers for <see cref="BrowseRule"/>. String paths and patterns are checked when the matcher is made.</summary>
public static class Match
{
    public static bool All(ObjectPath path) => true;

    /// <summary>Matches the last segment.</summary>
    public static Func<ObjectPath, bool> Name(string name) => p => p.Name == name;

    /// <summary>Matches an exact path, e.g. <c>"/M1/Components/Gripper"</c>.</summary>
    public static Func<ObjectPath, bool> Path(string path) => Path(ObjectPath.Parse(path));

    public static Func<ObjectPath, bool> Path(ObjectPath path) => p => p == path;

    /// <summary>Matches a path and all of its descendants.</summary>
    public static Func<ObjectPath, bool> PathPrefix(string prefix) => PathPrefix(ObjectPath.Parse(prefix));

    public static Func<ObjectPath, bool> PathPrefix(ObjectPath prefix) => p => p.IsRelativeTo(prefix);

    /// <summary>Matches paths by glob pattern (see <see cref="ObjectPath.Match"/>), e.g. <c>"/M1/**/SkillSet/*"</c>.</summary>
    public static Func<ObjectPath, bool> Glob(string pattern)
    {
        var glob = new ObjectPath.Glob(pattern); // check the pattern now, not on the first browsed object
        return glob.IsMatch;
    }

    public static Func<ObjectPath, bool> Any(params Func<ObjectPath, bool>[] matchers) => p => matchers.Any(m => m(p));

    public static Func<ObjectPath, bool> AllOf(params Func<ObjectPath, bool>[] matchers) => p => matchers.All(m => m(p));

    public static Func<ObjectPath, bool> Not(Func<ObjectPath, bool> matcher) => p => !matcher(p);
}
