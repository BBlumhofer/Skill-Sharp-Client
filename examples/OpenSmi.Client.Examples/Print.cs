using System.Text.Json;

namespace OpenSmi.Client.Examples;

internal static class Print
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };

    public static string Json(object value) => JsonSerializer.Serialize(value, Indented);

    public static string Values(IReadOnlyDictionary<string, object?> values) =>
        "{" + string.Join(", ", values.Select(kv => $"{kv.Key}={kv.Value}")) + "}";
}
