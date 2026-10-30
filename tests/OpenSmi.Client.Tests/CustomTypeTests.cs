namespace OpenSmi.Client.Tests;

public sealed class CustomTypeServer : IAsyncLifetime
{
    public TestServer Server { get; private set; } = null!;
    public async Task InitializeAsync() => Server = await TestServer.StartAsync(4962, customTypes: true);
    public async Task DisposeAsync() => await Server.DisposeAsync();
}

/// <summary>Custom enum DataType <c>TestColor</c> {Red, Green, Blue}; expectations from the Python client (oracle, ticket 30).</summary>
public class CustomTypeTests(CustomTypeServer fixture) : IClassFixture<CustomTypeServer>
{
    [Fact]
    public async Task Enum_variable_exposes_type_name_valid_values_and_member_name()
    {
        await using var client = await new RemoteServer(fixture.Server.Options()).ConnectAsync();
        var variable = client.Machines["DummyMachine"].Components["DummyComponent"].ParameterSet["DummyEnumParameter"];

        Assert.Equal("TestColor", variable.UaDataType);
        Assert.Equal(new Dictionary<string, string> { ["0"] = "Red", ["1"] = "Green", ["2"] = "Blue" }, variable.ValidValues);
        Assert.Equal("Green", variable.Value);
    }

    [Fact]
    public async Task Enum_variable_is_written_by_member_name_or_number()
    {
        await using var client = await new RemoteServer(fixture.Server.Options()).ConnectAsync();
        var variable = client.Machines["DummyMachine"].Components["DummyComponent"].ParameterSet["DummyEnumParameter"];

        try
        {
            await variable.WriteValueAsync("Blue");
            Assert.Equal("Blue", await variable.ReadValueAsync());
            await variable.WriteValueAsync(0);
            Assert.Equal("Red", await variable.ReadValueAsync());
            await Assert.ThrowsAsync<OpenSmiInvalidValueException>(() => variable.WriteValueAsync("Purple"));
        }
        finally { await variable.WriteValueAsync("Green"); }
    }
}
