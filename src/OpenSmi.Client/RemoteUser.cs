using Opc.Ua;

namespace OpenSmi.Client;

/// <param name="UserLevel">Lock priority as the server writes it (string); higher may break the lock.</param>
public sealed record RemoteUserData(string Name, bool AllowMultiple, string UserLevel, int MaxAccessLevel, bool IsPresent);

/// <summary>Port of <c>client/remote_user.py</c>: an entry of MachineryBuildingBlocks/Users.</summary>
public sealed class RemoteUser : RemoteUaObject
{
    internal RemoteUser(string name, NodeId nodeId, RemoteServer server, RemoteMachine parent) : base(name, nodeId, server, parent)
    {
        Data = new(name, false, "", 0, false);
        UserPresenceChanged = new(Logger);
    }

    public RemoteUserData Data { get; private set; }
    public AsyncSignal<(RemoteUser Sender, bool IsPresent)> UserPresenceChanged { get; }

    /// <summary><see cref="RemoteUserData.UserLevel"/> as number, <c>null</c> if not numeric.</summary>
    public int? LockPriority => int.TryParse(Data.UserLevel, out var p) ? p : null;

    private protected override async Task InitCoreAsync(CancellationToken ct)
    {
        var children = await UaUtil.BrowseChildrenAsync(Session, NodeId, ct).ConfigureAwait(false);
        var names = new[] { "AllowMultiple", "UserLevel", "MaxAccessLevel", "IsPresent" };
        var ids = names.Select(n => children.Child(n) is { } c ? UaUtil.ToNodeId(Session, c.NodeId)
            : throw new OpenSmiProtocolException($"{Path}: user has no '{n}'")).ToList();
        var v = await UaUtil.ReadAsync(Session, ids.Select(i => (i, Opc.Ua.Attributes.Value)), ct).ConfigureAwait(false);
        Data = new(Name, v[0].Value is true, v[1].Value?.ToString() ?? "", Convert.ToInt32(v[2].Value ?? 0), v[3].Value is true);
        await Server.Subscriptions.SubscribeDataChangeAsync([(ids[3], OnPresenceAsync)], ct).ConfigureAwait(false);
    }

    private Task OnPresenceAsync(DataValue dv)
    {
        var present = dv.Value is true;
        if (present == Data.IsPresent) return Task.CompletedTask;
        Data = Data with { IsPresent = present };
        return UserPresenceChanged.SendAsync((this, present));
    }

    public override string ToString() => $"{Name} (present: {Data.IsPresent}, level: {Data.UserLevel})";
}
