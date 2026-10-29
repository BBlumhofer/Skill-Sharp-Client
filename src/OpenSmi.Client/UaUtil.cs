using Opc.Ua;
using Opc.Ua.Client;

namespace OpenSmi.Client;

/// <summary>Port of the parts of <c>core/ua_node_util.py</c> and <c>client/common.py</c> the client needs.</summary>
internal static class UaUtil
{
    public static NodeId ToNodeId(ISession session, ExpandedNodeId id) => ExpandedNodeId.ToNodeId(id, session.NamespaceUris);

    /// <summary>Hierarchical forward children (objects, variables, methods); empty if the node does not exist.</summary>
    public static async Task<IReadOnlyList<ReferenceDescription>> BrowseChildrenAsync(ISession session, NodeId nodeId, CancellationToken ct) =>
        (await BrowseChildrenAsync(session, [nodeId], ct).ConfigureAwait(false))[0];

    public static async Task<IReadOnlyList<IReadOnlyList<ReferenceDescription>>> BrowseChildrenAsync(
        ISession session, IList<NodeId> nodeIds, CancellationToken ct) =>
        await BrowseAsync(session, nodeIds, ReferenceTypeIds.HierarchicalReferences, BrowseDirection.Forward,
            (uint)(NodeClass.Object | NodeClass.Variable | NodeClass.Method), ct).ConfigureAwait(false);

    public static async Task<IReadOnlyList<IReadOnlyList<ReferenceDescription>>> BrowseAsync(ISession session,
        IList<NodeId> nodeIds, NodeId referenceType, BrowseDirection direction, uint nodeClassMask, CancellationToken ct)
    {
        var (results, errors) = await session.ManagedBrowseAsync(null, null, nodeIds, 0, direction, referenceType,
            true, nodeClassMask, ct).ConfigureAwait(false);
        return [.. results.Select((r, i) => ServiceResult.IsBad(errors[i]) || r == null ? [] : (IReadOnlyList<ReferenceDescription>)r)];
    }

    public static ReferenceDescription? Child(this IReadOnlyList<ReferenceDescription> refs, string name) =>
        refs.FirstOrDefault(r => r.BrowseName.Name == name);

    /// <summary>BrowseName of the HasTypeDefinition target, or "UNKNOWN".</summary>
    public static async Task<string> ReadTypeNameAsync(ISession session, NodeId nodeId, CancellationToken ct)
    {
        var refs = (await BrowseAsync(session, [nodeId], ReferenceTypeIds.HasTypeDefinition, BrowseDirection.Forward, 0, ct)
            .ConfigureAwait(false))[0];
        return refs.Count > 0 ? refs[0].BrowseName.Name : "UNKNOWN";
    }

    public static async Task<DataValueCollection> ReadAsync(ISession session, IEnumerable<(NodeId Node, uint Attribute)> ids, CancellationToken ct)
    {
        var request = new ReadValueIdCollection(ids.Select(i => new ReadValueId { NodeId = i.Node, AttributeId = i.Attribute }));
        var response = await session.ReadAsync(null, 0, TimestampsToReturn.Both, request, ct).ConfigureAwait(false);
        return response.Results;
    }

    /// <summary>Calls a method without input arguments; a bad status becomes an <see cref="OpenSmiStatusException"/>.</summary>
    public static async Task<VariantCollection> CallAsync(ISession session, NodeId objectId, NodeId methodId, string path, CancellationToken ct)
    {
        var response = await session.CallAsync(null,
            [new CallMethodRequest { ObjectId = objectId, MethodId = methodId }], ct).ConfigureAwait(false);
        var result = response.Results[0];
        OpenSmiStatusException.ThrowIfBad(result.StatusCode, $"Call of {path}");
        return result.OutputArguments;
    }

    /// <summary>
    /// Port of <c>common.gather</c>: runs all inits concurrently and collects ordinary failures instead of
    /// throwing. Connection loss, timeouts and cancellation still propagate.
    /// </summary>
    public static Task<List<Exception>> InitAllAsync(IEnumerable<RemoteUaObject> objects, CancellationToken ct) =>
        GatherAsync(objects.Select(o => ($"{o.Path} ({o.NodeId})", (Func<Task>)(() => o.InitAsync(ct)))));

    public static async Task<List<Exception>> GatherAsync(IEnumerable<(string Label, Func<Task> Run)> work)
    {
        var failures = new List<Exception>();
        await Task.WhenAll(work.Select(async w =>
        {
            try { await w.Run().ConfigureAwait(false); }
            catch (Exception e) when (!IsFatal(e))
            {
                lock (failures) failures.Add(new OpenSmiException($"Init of {w.Label} failed: {e.Message}", e));
            }
        })).ConfigureAwait(false);
        return failures;
    }

    private static bool IsFatal(Exception e) =>
        e is OperationCanceledException or TimeoutException or OpenSmiConnectionException
        || e is ServiceResultException s && s.StatusCode is StatusCodes.BadConnectionClosed or StatusCodes.BadTimeout
            or StatusCodes.BadSessionClosed or StatusCodes.BadNotConnected or StatusCodes.BadSecureChannelClosed;
}
