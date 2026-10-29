namespace OpenSmi.Client.Tests;

/// <summary>Expected ids and symbols are literals taken from opensmi-core's <c>unit_all.py</c>.</summary>
public class UnitTests
{
    [Fact]
    public void From_ua_unit_id_finds_centimetre()
    {
        var unit = Units.FromUaUnitId(4410708);

        Assert.Equal(Unit.CENTIMETRE, unit);
        Assert.Equal("cm", unit.Symbol());
    }

    [Fact]
    public void Unknown_ua_unit_id_throws() => Assert.Throws<ArgumentException>(() => Units.FromUaUnitId(42));

    [Fact]
    public void Eu_information_round_trips_like_python()
    {
        var eu = Unit.CENTIMETRE.ToEuInformation();

        Assert.Equal("http://www.opcfoundation.org/UA/units/un/cefact", eu.NamespaceUri);
        Assert.Equal(4410708, eu.UnitId);
        Assert.Equal("cm", eu.DisplayName.Text);
        Assert.Equal("CENTIMETRE", eu.Description.Text);
        Assert.Equal(Unit.CENTIMETRE, eu.ToUnit());
    }

    [Fact]
    public void From_symbol_finds_unique_and_rejects_unknown_or_ambiguous()
    {
        Assert.Equal(Unit.METRE, Units.FromSymbol("m"));
        Assert.Throws<ArgumentException>(() => Units.FromSymbol("no such unit"));
        var ambiguous = Assert.Throws<ArgumentException>(() => Units.FromSymbol("cal")); // CALORIE and CALORIE_MEAN
        Assert.Contains("Ambiguous", ambiguous.Message);
    }
}

[Collection("server")]
public class UnitServerTests
{
    [Fact]
    public async Task Dummy_int_parameter_is_in_centimetre()
    {
        await using var server = await ServerFixture.ConnectAsync();

        Assert.Equal(Unit.CENTIMETRE, server.Machines["DummyMachine"].ParameterSet["DummyIntParameter"].Unit?.ToUnit());
    }
}
