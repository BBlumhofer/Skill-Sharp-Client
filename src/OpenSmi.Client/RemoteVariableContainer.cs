using System.Collections;
using System.Diagnostics.CodeAnalysis;
using Opc.Ua;

namespace OpenSmi.Client;

/// <summary>
/// Port of <c>client/remote_variable_container.py</c>. Always present on its parent; stays empty when
/// the server lacks the node or the feature is not browsed.
/// </summary>
public abstract class RemoteVariableContainer : RemoteUaObject, IReadOnlyDictionary<string, RemoteVariable>
{
    private Dictionary<string, RemoteVariable> _variables = [];

    private protected RemoteVariableContainer(string name, RemoteServer server, RemoteUaObject parent)
        : base(name, null, server, parent) { }

    internal Task InitAsync(NodeId nodeId, CancellationToken ct)
    {
        SetNodeId(nodeId);
        return InitAsync(ct);
    }

    private protected override async Task InitCoreAsync(CancellationToken ct)
    {
        var children = await UaUtil.BrowseChildrenAsync(Session, NodeId, ct).ConfigureAwait(false);
        var variables = children.Where(c => c.NodeClass == NodeClass.Variable).Select(c =>
        {
            var id = UaUtil.ToNodeId(Session, c.NodeId);
            return Server.GetUaObject(id) as RemoteVariable ?? new RemoteVariable(c.BrowseName.Name, id, Server, this);
        }).ToList();
        var ok = await InitChildrenAsync(variables, ct).ConfigureAwait(false);
        await Task.WhenAll(ok.Select(v => v.ReadUaTypeAsync(ct))).ConfigureAwait(false);
        _variables = ok.ToDictionary(v => v.Name);
    }

    /// <summary>Reads all values in one request.</summary>
    public async Task<IReadOnlyDictionary<string, object?>> ReadAllAsync(CancellationToken cancellationToken = default)
    {
        var vars = _variables.Values.ToList();
        if (vars.Count == 0) return new Dictionary<string, object?>();
        var values = await UaUtil.ReadAsync(Session, vars.Select(v => (v.NodeId, Opc.Ua.Attributes.Value)), cancellationToken)
            .ConfigureAwait(false);
        return vars.Select((v, i) => (v, dv: values[i])).ToDictionary(x => x.v.Name, x =>
        {
            OpenSmiStatusException.ThrowIfBad(x.dv.StatusCode, $"Read of {x.v.Path}");
            return x.v.Process(x.dv.WrappedValue);
        });
    }

    /// <summary>Writes all values in one request; unknown names throw before anything is written.</summary>
    public async Task WriteAllAsync(IReadOnlyDictionary<string, object?> values, CancellationToken cancellationToken = default)
    {
        var vars = values.Keys.Select(k => this[k]).ToList();
        var writes = new WriteValueCollection(vars.Select(v => v.ToWriteValue(values[v.Name])));
        var response = await Session.WriteAsync(null, writes, cancellationToken).ConfigureAwait(false);
        // Rejected values go through the single write for precise exceptions.
        for (var i = 0; i < vars.Count; i++)
            if (StatusCode.IsBad(response.Results[i]))
                await vars[i].WriteValueAsync(values[vars[i].Name], cancellationToken).ConfigureAwait(false);
    }

    internal IEnumerable<(NodeId, Func<DataValue, Task>)> SubscriptionItems =>
        _variables.Values.Select(v => (v.NodeId, (Func<DataValue, Task>)v.OnDataChangeAsync));

    public RemoteVariable this[string key] =>
        _variables.TryGetValue(key, out var v) ? v : throw new KeyNotFoundException($"No variable '{key}' in {Path}");

    public IEnumerable<string> Keys => _variables.Keys;
    public IEnumerable<RemoteVariable> Values => _variables.Values;
    public int Count => _variables.Count;
    public bool ContainsKey(string key) => _variables.ContainsKey(key);
    public bool TryGetValue(string key, [MaybeNullWhen(false)] out RemoteVariable value) => _variables.TryGetValue(key, out value);
    public IEnumerator<KeyValuePair<string, RemoteVariable>> GetEnumerator() => _variables.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

public sealed class Attributes(RemoteServer server, RemoteUaObject parent) : RemoteVariableContainer(nameof(Attributes), server, parent);
public sealed class Identification(RemoteServer server, RemoteUaObject parent) : RemoteVariableContainer(nameof(Identification), server, parent);
public sealed class Monitoring(RemoteServer server, RemoteUaObject parent) : RemoteVariableContainer(nameof(Monitoring), server, parent);
public sealed class ParameterSet(RemoteServer server, RemoteUaObject parent) : RemoteVariableContainer(nameof(ParameterSet), server, parent);
public sealed class FinalResultData(RemoteServer server, RemoteUaObject parent) : RemoteVariableContainer(nameof(FinalResultData), server, parent);
