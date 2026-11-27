using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Text.RegularExpressions;

namespace OpenSmi.Client;

/// <summary>
/// Port of opensmi-core 0.7 <c>paths.ObjectPath</c>: path of an OpenSMI object such as
/// <c>/M1/Components/Gripper/SkillSet/Pick</c>. A sequence of segments, never normalized; equal by segments and
/// ordered segment by segment (ordinal), which gives tree order.
/// </summary>
public sealed partial class ObjectPath : IEquatable<ObjectPath>, IComparable<ObjectPath>
{
    private const char Separator = '/';
    private const string AnyDepth = "**";
    private readonly string[] _segments;

    /// <summary>The root <c>/</c>.</summary>
    public static ObjectPath Root { get; } = new([]);

    private ObjectPath(string[] segments) => _segments = segments;

    /// <exception cref="ArgumentException">A segment is empty or contains <c>/</c>.</exception>
    public ObjectPath(IEnumerable<string> segments) : this([.. segments])
    {
        foreach (var segment in _segments) CheckSegment(segment);
    }

    public IReadOnlyList<string> Segments => _segments;

    /// <summary>Last segment; <c>""</c> for the root.</summary>
    public string Name => _segments.Length > 0 ? _segments[^1] : "";

    /// <exception cref="InvalidOperationException">For the root, which has no parent.</exception>
    public ObjectPath Parent => _segments.Length > 0
        ? new ObjectPath(_segments[..^1])
        : throw new InvalidOperationException("The root path has no parent");

    /// <summary>Parses <c>"/M1/Components/Gripper"</c>; <c>"/"</c> is the root.</summary>
    /// <exception cref="ArgumentException">No leading <c>/</c>, or an empty segment (<c>//</c>, trailing <c>/</c>).</exception>
    public static ObjectPath Parse(string text) => new((IEnumerable<string>)Split(text));

    /// <summary>This path extended by <paramref name="segment"/>; the segment is checked, not sanitized.</summary>
    public static ObjectPath operator /(ObjectPath path, string segment)
    {
        CheckSegment(segment);
        return new([.. path._segments, segment]);
    }

    /// <summary>Whether this path is <paramref name="other"/> or below it.</summary>
    public bool IsRelativeTo(ObjectPath other) =>
        other._segments.Length <= _segments.Length && other._segments.AsSpan().SequenceEqual(_segments.AsSpan(0, other._segments.Length));

    public bool IsRelativeTo(string other) => IsRelativeTo(Parse(other));

    /// <summary>
    /// Whether the whole path matches the glob <paramref name="pattern"/>. Per segment <c>*</c>, <c>?</c> and
    /// <c>[...]</c> work as in Python's <c>fnmatchcase</c>; a <c>**</c> segment matches any number of segments,
    /// including none. Use <see cref="Escape"/> for segments taken literally.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="pattern"/> is no valid path pattern.</exception>
    public bool Match(string pattern) => new Glob(pattern).IsMatch(this);

    /// <summary><paramref name="segment"/> as a glob pattern matching only itself (<c>*</c> → <c>[*]</c>, ...).</summary>
    public static string Escape(string segment) => GlobSpecial().Replace(segment, "[$1]");

    public bool Equals(ObjectPath? other) => other is not null && _segments.AsSpan().SequenceEqual(other._segments);
    public override bool Equals(object? obj) => Equals(obj as ObjectPath);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var segment in _segments) hash.Add(segment, StringComparer.Ordinal);
        return hash.ToHashCode();
    }

    public int CompareTo(ObjectPath? other)
    {
        if (other is null) return 1;
        for (var i = 0; i < Math.Min(_segments.Length, other._segments.Length); i++)
        {
            var c = string.CompareOrdinal(_segments[i], other._segments[i]);
            if (c != 0) return c;
        }
        return _segments.Length.CompareTo(other._segments.Length);
    }

    public static bool operator ==(ObjectPath? a, ObjectPath? b) => a?.Equals(b) ?? b is null;
    public static bool operator !=(ObjectPath? a, ObjectPath? b) => !(a == b);

    public override string ToString() => Separator + string.Join(Separator, _segments);

    internal static void CheckSegment(string segment)
    {
        if (string.IsNullOrEmpty(segment)) throw new ArgumentException("Path segment must not be empty", nameof(segment));
        if (segment.Contains(Separator)) throw new ArgumentException($"Path segment must not contain '/': '{segment}'", nameof(segment));
    }

    private static string[] Split(string text)
    {
        if (!text.StartsWith(Separator)) throw new ArgumentException($"Path must start with '/': '{text}'", nameof(text));
        return text == "/" ? [] : text[1..].Split(Separator);
    }

    [GeneratedRegex(@"([*?\[])")]
    private static partial Regex GlobSpecial();

    /// <summary>A checked pattern; segments other than <c>**</c> are translated like <c>fnmatch.translate</c>.</summary>
    internal sealed class Glob
    {
        private readonly Regex?[] _segments; // null = "**"

        public Glob(string pattern)
        {
            var parts = Split(pattern);
            foreach (var part in parts) CheckSegment(part);
            _segments = [.. parts.Select(p => p == AnyDepth ? null : new Regex(Translate(p), RegexOptions.Singleline | RegexOptions.CultureInvariant))];
        }

        public bool IsMatch(ObjectPath path)
        {
            var segments = path._segments;
            var memo = new Dictionary<(int, int), bool>();
            bool Matches(int i, int j)
            {
                if (memo.TryGetValue((i, j), out var known)) return known;
                bool result;
                if (j == _segments.Length) result = i == segments.Length;
                else if (_segments[j] == null) result = Enumerable.Range(i, segments.Length - i + 1).Any(k => Matches(k, j + 1));
                else result = i < segments.Length && _segments[j]!.IsMatch(segments[i]) && Matches(i + 1, j + 1);
                return memo[(i, j)] = result;
            }
            return Matches(0, 0);
        }

        private static string Translate(string pattern)
        {
            var sb = new StringBuilder(@"\A(?:");
            for (var i = 0; i < pattern.Length; i++)
            {
                var c = pattern[i];
                switch (c)
                {
                    case '*': sb.Append(".*"); break;
                    case '?': sb.Append('.'); break;
                    case '[':
                        var j = i + 1;
                        if (j < pattern.Length && pattern[j] == '!') j++;
                        if (j < pattern.Length && pattern[j] == ']') j++;
                        while (j < pattern.Length && pattern[j] != ']') j++;
                        if (j >= pattern.Length) { sb.Append(@"\["); break; } // unclosed: literal '['
                        var set = pattern[(i + 1)..j].Replace(@"\", @"\\");
                        i = j;
                        if (set.StartsWith('!')) set = "^" + set[1..];
                        else if (set.StartsWith('^')) set = @"\" + set;
                        sb.Append('[').Append(set.Replace("[", @"\[")).Append(']');
                        break;
                    default: sb.Append(Regex.Escape(c.ToString())); break;
                }
            }
            return sb.Append(@")\z").ToString();
        }
    }
}

/// <summary>An OPC UA NodeId independent of the server's namespace indexes (opensmi-core <c>NodeIdDefinition</c>).</summary>
/// <param name="Identifier"><see cref="uint"/>, <see cref="string"/>, <see cref="Guid"/> or <see cref="T:byte[]"/>.</param>
internal readonly record struct NodeIdKey(string NamespaceUri, object Identifier) : IComparable<NodeIdKey>
{
    /// <summary>Like <c>node_id_order</c>: namespace URI, then identifier type (numeric, string, guid, opaque), then identifier.</summary>
    public int CompareTo(NodeIdKey other)
    {
        var c = string.CompareOrdinal(NamespaceUri, other.NamespaceUri);
        if (c != 0) return c;
        c = Rank(Identifier).CompareTo(Rank(other.Identifier));
        if (c != 0) return c;
        return (Identifier, other.Identifier) switch
        {
            (uint a, uint b) => a.CompareTo(b),
            (string a, string b) => string.CompareOrdinal(a, b),
            (Guid a, Guid b) => string.CompareOrdinal(a.ToString(), b.ToString()),
            (byte[] a, byte[] b) => string.CompareOrdinal(Convert.ToHexString(a).ToLowerInvariant(), Convert.ToHexString(b).ToLowerInvariant()),
            _ => 0,
        };
    }

    private static int Rank(object identifier) => identifier switch { uint => 0, string => 1, Guid => 2, _ => 3 };

    public bool Equals(NodeIdKey other) =>
        NamespaceUri == other.NamespaceUri && (Identifier is byte[] a && other.Identifier is byte[] b ? a.AsSpan().SequenceEqual(b) : Identifier.Equals(other.Identifier));

    public override int GetHashCode() =>
        HashCode.Combine(NamespaceUri, Identifier is byte[] bytes ? Convert.ToHexString(bytes).GetHashCode() : Identifier.GetHashCode());
}

/// <summary>Port of opensmi-core 0.7 <c>paths.sanitize_segment</c> and <c>unique_segments</c>.</summary>
internal static class PathSegments
{
    /// <summary>The segment for <paramref name="browseName"/>: <c>/</c> replaced by <c>_</c>, an empty name becomes <c>_</c>.</summary>
    public static string Sanitize(string browseName) => browseName.Length == 0 ? "_" : browseName.Replace('/', '_');

    /// <summary>
    /// A sanitized, unique segment per sibling. Siblings are processed in NodeId order, so the result does not depend
    /// on browse order or namespace indexes; a taken segment gets the first free suffix <c>_2</c>, <c>_3</c>, ..., and
    /// a suffixed segment never takes another sibling's unchanged segment.
    /// </summary>
    public static Dictionary<NodeIdKey, string> Unique(IEnumerable<(NodeIdKey Id, string BrowseName)> siblings, ILogger? logger = null)
    {
        var sanitized = siblings.OrderBy(s => s.Id).Select(s => (s.Id, s.BrowseName, Segment: Sanitize(s.BrowseName))).ToList();
        var reserved = sanitized.Select(s => s.Segment).ToHashSet();
        var used = new HashSet<string>();
        logger ??= NullLogger.Instance;
        var result = new Dictionary<NodeIdKey, string>();
        foreach (var (id, name, segment) in sanitized)
        {
            if (segment != name) logger.LogWarning("Browse name '{BrowseName}' is no valid path segment, using '{Segment}'", name, segment);
            var candidate = segment;
            for (var suffix = 2; used.Contains(candidate) || (candidate != segment && reserved.Contains(candidate)); suffix++)
                candidate = $"{segment}_{suffix}";
            if (candidate != segment) logger.LogWarning("Duplicate path segment '{BrowseName}' of {NodeId}, using '{Segment}'", name, id, candidate);
            used.Add(candidate);
            result[id] = candidate;
        }
        return result;
    }
}
