namespace OpenSmi.Client;

/// <summary>Port of <c>client/browse.py</c>. Paths are POSIX strings such as <c>/Machine/Component</c>.</summary>
public sealed record BrowseRule(Func<string, bool> Match, BrowseFeature Features)
{
    public static BrowseRule Default { get; } = new(OpenSmi.Client.Match.All, BrowseFeature.All);
}

public static class BrowseRules
{
    public static BrowseRule When(Func<string, bool> match, params BrowseFeature[] features) =>
        new(match, features.Aggregate(BrowseFeature.None, (a, f) => a | f));

    public static BrowseFeature Evaluate(IEnumerable<BrowseRule> rules, string path) =>
        rules.Where(r => r.Match(path)).Aggregate(BrowseFeature.None, (a, r) => a | r.Features);
}

public static class Match
{
    public static bool All(string path) => true;

    public static Func<string, bool> Name(string name) => p => p[(p.LastIndexOf('/') + 1)..] == name;

    public static Func<string, bool> Path(string path) => p => p == path;

    public static Func<string, bool> PathPrefix(string prefix) =>
        p => p == prefix || p.StartsWith(prefix.TrimEnd('/') + "/", StringComparison.Ordinal);

    public static Func<string, bool> Any(params Func<string, bool>[] matchers) => p => matchers.Any(m => m(p));

    public static Func<string, bool> AllOf(params Func<string, bool>[] matchers) => p => matchers.All(m => m(p));

    public static Func<string, bool> Not(Func<string, bool> matcher) => p => !matcher(p);
}
