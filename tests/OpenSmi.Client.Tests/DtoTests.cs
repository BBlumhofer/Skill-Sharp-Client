using System.Text.Json;
using System.Text.Json.Nodes;

namespace OpenSmi.Client.Tests;

[Collection("server")]
public class DtoTests
{
    [Fact]
    public async Task Machine_dto_has_the_same_objects_as_the_python_client()
    {
        await using var server = await ServerFixture.ConnectAsync();
        var dto = server.Machines["DummyMachine"].ToDto();

        var json = JsonNode.Parse(JsonSerializer.Serialize(dto))!;
        var ours = new SortedSet<string>(StringComparer.Ordinal);
        Walk(json, ours);
        var oracle = File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "oracle", "dummy-machine-paths.txt"))
            .Where(l => l.Length > 0).ToHashSet();

        Assert.Empty(oracle.Except(ours));
        // Python leaves feasibility/precondition checks out of the DTO (SPEC §10).
        Assert.All(ours.Except(oracle), extra => Assert.Matches(@"\.(FeasibilityCheck|PreconditionCheck)\b", extra));
        Assert.Equal("orchestrator", dto.Users.Single(u => u.Name == "orchestrator").Name);
        Assert.Equal(10, dto.Users.Single(u => u.Name == "orchestrator").LockPriority);
    }

    [Fact]
    public async Task Machine_dto_without_browsed_lock_has_no_lock()
    {
        await using var server = await ServerFixture.ConnectAsync(rules: [BrowseRules.When(Match.All, BrowseFeature.Machines)]);
        Assert.Null(server.Machines["DummyMachine"].ToDto().Lock);
    }

    private static void Walk(JsonNode? node, ISet<string> found)
    {
        switch (node)
        {
            case JsonObject o:
                if (o["Path"] is JsonValue p && o["UaNodeId"] is JsonValue id) found.Add($"{p} {id}");
                foreach (var (_, child) in o) Walk(child, found);
                break;
            case JsonArray a:
                foreach (var child in a) Walk(child, found);
                break;
        }
    }
}
