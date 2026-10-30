using Opc.Ua;

namespace OpenSmi.Client;

/// <summary>
/// Port of <c>core/unit_base.py</c>. The UA unit id is <c>(int)unit</c>. Use <see cref="ToUnit"/> on
/// <see cref="RemoteVariable.Unit"/> to get the enum.
/// </summary>
public static partial class Units
{
    private const string NamespaceUri = "http://www.opcfoundation.org/UA/units/un/cefact";

    public static string Symbol(this Unit unit) => Info[unit].Symbol;

    /// <exception cref="ArgumentException">Unknown unit id.</exception>
    public static Unit FromUaUnitId(int uaUnitId) => Enum.IsDefined(typeof(Unit), uaUnitId)
        ? (Unit)uaUnitId
        : throw new ArgumentException($"Unknown OPC UA unit ID: {uaUnitId}", nameof(uaUnitId));

    /// <exception cref="ArgumentException">Unknown or ambiguous symbol.</exception>
    public static Unit FromSymbol(string symbol)
    {
        var matches = Info.Where(kv => kv.Value.Symbol == symbol).Select(kv => kv.Key).ToList();
        return matches.Count switch
        {
            0 => throw new ArgumentException($"Unknown Unit symbol: {symbol}", nameof(symbol)),
            1 => matches[0],
            _ => throw new ArgumentException(
                $"Ambiguous Unit symbol '{symbol}'; unit IDs: {string.Join(", ", matches.Select(u => (int)u))}", nameof(symbol)),
        };
    }

    /// <summary>The description is the opensmi-core name (e.g. <c>CENTIMETRE</c>), like Python.</summary>
    public static EUInformation ToEuInformation(this Unit unit) =>
        new(unit.Symbol(), Info[unit].Name, NamespaceUri) { UnitId = (int)unit };

    /// <exception cref="ArgumentException">Unknown unit id.</exception>
    public static Unit ToUnit(this EUInformation euInformation) => FromUaUnitId(euInformation.UnitId);
}
