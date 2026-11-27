using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Opc.Ua;
using Opc.Ua.Client;

namespace OpenSmi.Client;

public sealed record RemoteServerOptions
{
    public required string Url { get; init; }
    public required string Username { get; init; }
    public required string Password { get; init; }
    public string ApplicationUri { get; init; } = $"urn:uuid:{Guid.NewGuid()}";
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(10);
    /// <summary>Period of the session keep-alive; a dead server is noticed after about this plus one second.</summary>
    public TimeSpan WatchdogInterval { get; init; } = TimeSpan.FromSeconds(1);
    public SecurityOptions Security { get; init; } = SecurityOptions.None;
    /// <summary><c>null</c> browses everything, an empty list browses nothing.</summary>
    public IReadOnlyList<BrowseRule>? BrowseRules { get; init; }
    public TimeSpan PublishingInterval { get; init; } = TimeSpan.FromMilliseconds(100);
    public bool AutoReconnect { get; init; } = true;
    /// <summary>Upper bound of the reconnect backoff; the SDK caps it at 30 s.</summary>
    public TimeSpan ReconnectMaxDelay { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Without any notification for this long the subscription is deleted and rebuilt with all items.</summary>
    public TimeSpan HeartbeatStaleAfter { get; init; } = TimeSpan.FromSeconds(10);
    public ILoggerFactory? LoggerFactory { get; init; }
}

/// <summary><see cref="MaxRetries"/> = <c>null</c> retries forever.</summary>
public sealed record ConnectRetryOptions(int? MaxRetries = 3, double MinDelaySeconds = 0.5, double MaxDelaySeconds = 30);

/// <summary>Port of <c>client/remote_server.py</c>.</summary>
public sealed class RemoteServer : IAsyncDisposable
{
    private const string MachineryNamespace = "http://opcfoundation.org/UA/Machinery/";
    private readonly RemoteServerOptions _options;
    private readonly Dictionary<NodeId, WeakReference<RemoteUaObject>> _uaObjects = [];
    private readonly Dictionary<ObjectPath, WeakReference<RemoteUaObject>> _uaObjectsByPath = [];
    private readonly List<(ObjectPath Path, NodeId Node, Action<RemoteUaObject> Attach)> _pendingAliases = [];
    private Dictionary<string, RemoteMachine> _machines = [];
    private Dictionary<string, RemoteUser> _users = [];
    private readonly List<Exception> _initExceptions = [];
    private SessionReconnectHandler? _reconnect;
    private readonly object _disconnectGate = new();
    private Task? _disconnect;
    private readonly AsyncLocal<bool> _inDisconnect = new();

    public RemoteServer(RemoteServerOptions options)
    {
        _options = options;
        LoggerFactory = options.LoggerFactory ?? NullLoggerFactory.Instance;
        Logger = LoggerFactory.CreateLogger<RemoteServer>();
        BrowseRules = options.BrowseRules ?? [BrowseRule.Default];
    }

    internal ILoggerFactory LoggerFactory { get; }
    internal ILogger Logger { get; }
    internal SubscriptionManager Subscriptions { get; private set; } = null!;

    public RemoteServerStatus Status { get; private set; }
    public bool IsConnected => Session is { Connected: true };
    /// <summary>Browsed machines by path segment (their name, unless sanitized or suffixed).</summary>
    public IReadOnlyDictionary<string, RemoteMachine> Machines => _machines;
    /// <summary>Users of the server by path segment; empty if not browsed (<see cref="BrowseFeature.Users"/> at <c>/</c>).</summary>
    public IReadOnlyDictionary<string, RemoteUser> Users => _users;
    /// <summary>Always the root <c>/</c>.</summary>
    public ObjectPath Path => ObjectPath.Root;
    public string Username => _options.Username;
    public IReadOnlyList<BrowseRule> BrowseRules { get; }
    public IReadOnlyList<Exception> InitExceptions => _initExceptions;
    /// <summary>Escape hatch to the raw OPC UA session (Python: <c>ua_client</c>).</summary>
    public ISession? Session { get; private set; }

    public event AsyncEventHandler<ServerStatusChangedEventArgs>? StatusChanged;
    /// <summary>Server_ServerStatus_CurrentTime ticks, usable as heartbeat.</summary>
    public event AsyncEventHandler<ServerTimeUpdatedEventArgs>? TimeUpdated;
    private readonly Watchers<RemoteServerStatus> _statusWatchers = new();

    /// <summary>Yields <see cref="Status"/> first, then every change; a slow reader skips the oldest changes.</summary>
    public IAsyncEnumerable<RemoteServerStatus> WatchStatusAsync(CancellationToken cancellationToken = default) =>
        _statusWatchers.WatchAsync(() => Status, cancellationToken);

    public async Task<RemoteServer> ConnectAsync(ConnectRetryOptions? retry = null, CancellationToken cancellationToken = default)
    {
        if (Status != RemoteServerStatus.Disconnected)
            throw new InvalidOperationException($"Cannot connect while {Status}");
        retry ??= new();
        await SetStatusAsync(RemoteServerStatus.Connecting).ConfigureAwait(false);
        try
        {
            var delay = retry.MinDelaySeconds;
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    Session = await CreateSessionAsync(cancellationToken).ConfigureAwait(false);
                    if (_options.AutoReconnect)
                        _reconnect = new SessionReconnectHandler(true, (int)_options.ReconnectMaxDelay.TotalMilliseconds);
                    Session.KeepAliveInterval = (int)_options.WatchdogInterval.TotalMilliseconds;
                    Session.KeepAlive += OnKeepAlive;
                    break;
                }
                catch (ServiceResultException e) when (e.StatusCode is StatusCodes.BadUserAccessDenied or StatusCodes.BadIdentityTokenInvalid)
                {
                    throw new OpenSmiAuthenticationException($"Login as '{Username}' rejected", e);
                }
                catch (ServiceResultException e) when (e.StatusCode == StatusCodes.BadCertificateUntrusted)
                {
                    throw new OpenSmiConnectionException($"Server certificate of {_options.Url} is not trusted", e);
                }
                catch (Exception e) when (e is ServiceResultException or System.Net.Sockets.SocketException)
                {
                    if (attempt >= retry.MaxRetries)
                        throw new OpenSmiConnectionException($"Could not connect to {_options.Url} after {attempt} attempts", e);
                    Logger.LogWarning(e, "Connect attempt {Attempt} failed, retrying in {Delay}s", attempt, delay);
                    await Task.Delay(TimeSpan.FromSeconds(delay), cancellationToken).ConfigureAwait(false);
                    delay = Math.Min(delay * 2, retry.MaxDelaySeconds);
                }
            }

            lock (_uaObjects)
            {
                _uaObjects.Clear();
                _uaObjectsByPath.Clear();
            }
            lock (_pendingAliases) _pendingAliases.Clear();
            _machines = [];
            _users = [];
            _initExceptions.Clear();
            Subscriptions = await SubscriptionManager.CreateAsync(this, _options.PublishingInterval, _options.HeartbeatStaleAfter, cancellationToken).ConfigureAwait(false);
            if (BrowseRules.Count > 0)
            {
                await SetStatusAsync(RemoteServerStatus.Browsing).ConfigureAwait(false);
                var machineNodes = await BrowseMachinesAsync(cancellationToken).ConfigureAwait(false);
                await BrowseUsersAsync(machineNodes, cancellationToken).ConfigureAwait(false);
            }
            await Subscriptions.SubscribeDataChangeAsync(
                [(VariableIds.Server_ServerStatus_CurrentTime, v => TimeUpdated.RaiseAsync(this, new(v.GetValue(DateTime.MinValue)), Logger))],
                cancellationToken).ConfigureAwait(false);
            await SetStatusAsync(RemoteServerStatus.Connected).ConfigureAwait(false);
            return this;
        }
        catch
        {
            await TearDownAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Concurrent calls share one teardown; a StatusChanged handler inside that teardown returns at once.</summary>
    public Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (_inDisconnect.Value) return Task.CompletedTask; // a handler cannot wait for the teardown that awaits it
        lock (_disconnectGate)
        {
            if (Status == RemoteServerStatus.Disconnected) return Task.CompletedTask;
            if (_disconnect is not { IsCompleted: false })
                _disconnect = Task.Run(async () =>
                {
                    _inDisconnect.Value = true;
                    await SetStatusAsync(RemoteServerStatus.Disconnecting).ConfigureAwait(false);
                    _inDisconnect.Value = false; // not into whatever a Disconnected handler starts
                    await TearDownAsync().ConfigureAwait(false);
                });
            return _disconnect;
        }
    }

    public ValueTask DisposeAsync() => new(DisconnectAsync());

    public ushort GetNamespaceIndex(string namespaceUri)
    {
        var index = RequireSession().NamespaceUris.GetIndex(namespaceUri);
        return index >= 0 ? (ushort)index : throw new KeyNotFoundException($"Namespace '{namespaceUri}' not on server");
    }

    public RemoteUaObject? GetUaObject(NodeId nodeId)
    {
        lock (_uaObjects)
            return _uaObjects.TryGetValue(nodeId, out var r) && r.TryGetTarget(out var o) ? o : null;
    }

    /// <summary>The object at <paramref name="path"/>: its canonical path or one of its aliases.</summary>
    /// <exception cref="KeyNotFoundException">No object has this path.</exception>
    public RemoteUaObject Resolve(ObjectPath path) =>
        ResolveOptional(path) ?? throw new KeyNotFoundException($"No object at {path}");

    /// <exception cref="ArgumentException"><paramref name="path"/> is no valid path.</exception>
    public RemoteUaObject Resolve(string path) => Resolve(ObjectPath.Parse(path));

    public RemoteUaObject? ResolveOptional(ObjectPath path)
    {
        lock (_uaObjects)
            return _uaObjectsByPath.TryGetValue(path, out var r) && r.TryGetTarget(out var o) ? o : null;
    }

    public RemoteUaObject? ResolveOptional(string path) => ResolveOptional(ObjectPath.Parse(path));

    /// <summary>
    /// All objects whose canonical path matches the glob <paramref name="pattern"/> (see <see cref="ObjectPath.Match"/>),
    /// in tree order, e.g. <c>Find("/M1/**/SkillSet/*")</c>. An object with aliases is listed once.
    /// </summary>
    public IReadOnlyList<RemoteUaObject> Find(string pattern)
    {
        var glob = new ObjectPath.Glob(pattern);
        lock (_uaObjects)
            return [.. _uaObjectsByPath
                .Select(kv => (Path: kv.Key, Object: kv.Value.TryGetTarget(out var o) ? o : null))
                .Where(x => x.Object != null && x.Path == x.Object.Path && glob.IsMatch(x.Path))
                .OrderBy(x => x.Path).Select(x => x.Object!)];
    }

    /// <summary>Registers by node and by canonical path; a parent must be registered before its children.</summary>
    internal void Register(RemoteUaObject obj)
    {
        lock (_uaObjects) _uaObjects[obj.NodeId] = new(obj);
        RegisterPath(obj.Path, obj);
    }

    /// <summary>A path already taken by another object keeps that object.</summary>
    internal void RegisterPath(ObjectPath path, RemoteUaObject obj)
    {
        lock (_uaObjects)
        {
            if (_uaObjectsByPath.TryGetValue(path, out var r) && r.TryGetTarget(out var existing) && !ReferenceEquals(existing, obj))
            {
                Logger.LogError("Path {Path} already registered for {Existing}, keeping it", path, existing);
                return;
            }
            _uaObjectsByPath[path] = new(obj);
        }
    }

    /// <summary>Remembers that <paramref name="node"/>, owned elsewhere, is also listed at <paramref name="path"/>.</summary>
    internal void AddPendingAlias(ObjectPath path, NodeId node, Action<RemoteUaObject> attach)
    {
        lock (_pendingAliases) _pendingAliases.Add((path, node, attach));
    }

    /// <summary>Attaches the aliases found while browsing, now that every owner is browsed.</summary>
    private void AttachAliases()
    {
        lock (_pendingAliases)
        {
            foreach (var (path, node, attach) in _pendingAliases)
            {
                if (GetUaObject(node) is not { } obj)
                {
                    Logger.LogWarning("Node {NodeId} listed at {Path}, but its owner wasn't browsed", node, path);
                    continue;
                }
                obj.AddAlias(path);
                attach(obj);
            }
            _pendingAliases.Clear();
        }
    }

    internal ISession RequireSession() => Session ?? throw new InvalidOperationException("Not connected");

    /// <returns>The nodes of all machines, browsed or not.</returns>
    private async Task<IReadOnlyList<ReferenceDescription>> BrowseMachinesAsync(CancellationToken ct)
    {
        var session = RequireSession();
        var machineNodes = await UaUtil.BrowseChildrenAsync(session,
            new NodeId(1001, GetNamespaceIndexOrZero(MachineryNamespace)), ct).ConfigureAwait(false);
        if (machineNodes.Count == 0)
        {
            var folder = (await UaUtil.BrowseChildrenAsync(session, ObjectIds.ObjectsFolder, ct).ConfigureAwait(false))
                .FirstOrDefault(r => r.BrowseName.Name == "Machines")
                ?? throw new OpenSmiProtocolException("No 'Machines' folder below Objects");
            machineNodes = await UaUtil.BrowseChildrenAsync(session, UaUtil.ToNodeId(session, folder.NodeId), ct).ConfigureAwait(false);
        }

        var segments = UaUtil.UniqueSegments(session, machineNodes, Logger);
        var machines = machineNodes
            .Select(r => (Ref: r, Id: UaUtil.ToNodeId(session, r.NodeId)))
            .Where(m => OpenSmi.Client.BrowseRules.Evaluate(BrowseRules, Path / segments[m.Id]).HasFlag(BrowseFeature.Machines))
            .Select(m => new RemoteMachine(m.Ref.BrowseName.Name ?? "", segments[m.Id], m.Id, this, BrowseRules))
            .ToList();
        var failures = await UaUtil.InitAllAsync(machines, ct).ConfigureAwait(false);
        _initExceptions.AddRange(failures);
        _machines = machines.Where(m => m.IsInitialized).ToDictionary(m => m.Segment);
        AttachAliases();
        return machineNodes;
    }

    /// <summary>
    /// Users from the server-wide <c>Objects/Users</c> (BrowseName <c>Users</c> in the MachineSet namespace). Servers
    /// without it (opensmi-server &lt; 0.3) list them below each machine's <c>MachineryBuildingBlocks/Users</c>; then
    /// they are read from there, one object per node.
    /// </summary>
    private async Task BrowseUsersAsync(IReadOnlyList<ReferenceDescription> machineNodes, CancellationToken ct)
    {
        if (!OpenSmi.Client.BrowseRules.Evaluate(BrowseRules, Path).HasFlag(BrowseFeature.Users))
        {
            Logger.LogInformation("Skipped browsing users");
            return;
        }
        var session = RequireSession();
        var machineSet = session.NamespaceUris.GetIndex(SubscriptionManager.MachineSetNamespace);
        var folders = (await UaUtil.BrowseChildrenAsync(session, ObjectIds.ObjectsFolder, ct).ConfigureAwait(false))
            .Where(r => r.BrowseName.Name == "Users" && r.BrowseName.NamespaceIndex == machineSet)
            .Select(r => UaUtil.ToNodeId(session, r.NodeId)).ToList();
        if (folders.Count == 0)
        {
            Logger.LogInformation("Server has no Objects/Users, reading users from the machines");
            var blocks = (await UaUtil.BrowseChildrenAsync(session, [.. machineNodes.Select(m => UaUtil.ToNodeId(session, m.NodeId))], ct)
                .ConfigureAwait(false)).Select(c => c.Child("MachineryBuildingBlocks")).OfType<ReferenceDescription>()
                .Select(b => UaUtil.ToNodeId(session, b.NodeId)).ToList();
            folders = [.. (await UaUtil.BrowseChildrenAsync(session, blocks, ct).ConfigureAwait(false))
                .Select(c => c.Child("Users")).OfType<ReferenceDescription>().Select(u => UaUtil.ToNodeId(session, u.NodeId))];
        }

        var userRefs = (await UaUtil.BrowseChildrenAsync(session, folders, ct).ConfigureAwait(false)).SelectMany(r => r)
            .DistinctBy(r => UaUtil.ToNodeId(session, r.NodeId)).ToList();
        var segments = UaUtil.UniqueSegments(session, userRefs, Logger);
        var users = userRefs.Select(r =>
        {
            var id = UaUtil.ToNodeId(session, r.NodeId);
            return new RemoteUser(r.BrowseName.Name ?? "", segments[id], id, this);
        }).ToList();
        _initExceptions.AddRange(await UaUtil.InitAllAsync(users, ct).ConfigureAwait(false));
        await Task.WhenAll(users.Where(u => u.IsInitialized).Select(u => u.ReadUaTypeAsync(ct))).ConfigureAwait(false);
        _users = users.Where(u => u.IsInitialized).ToDictionary(u => u.Segment);
    }

    private ushort GetNamespaceIndexOrZero(string uri) =>
        (ushort)Math.Max(0, RequireSession().NamespaceUris.GetIndex(uri));

    private async Task<ISession> CreateSessionAsync(CancellationToken ct)
    {
        var security = _options.Security;
        var config = new ApplicationConfiguration
        {
            ApplicationName = "OpenSMI C# Client",
            ApplicationUri = _options.ApplicationUri,
            ProductUri = "urn:opensmi:client",
            ApplicationType = ApplicationType.Client,
            SecurityConfiguration = new SecurityConfiguration { ApplicationCertificate = new CertificateIdentifier() },
            TransportQuotas = new TransportQuotas { OperationTimeout = (int)_options.RequestTimeout.TotalMilliseconds },
            ClientConfiguration = new ClientConfiguration { DefaultSessionTimeout = 60_000 },
            CertificateValidator = new CertificateValidator(),
        };
        if (!security.IsNone) await ConfigureSecurityAsync(config, security, ct).ConfigureAwait(false);

        var endpoints = await GetEndpointsAsync(config, ct).ConfigureAwait(false);
        var endpoint = endpoints.FirstOrDefault(e => e.SecurityPolicyUri == security.SecurityPolicyUri && e.SecurityMode == security.Mode)
            ?? throw new OpenSmiConnectionException(
                $"{_options.Url} offers no endpoint with {security.SecurityPolicyUri} / {security.Mode}");
        return await DefaultSessionFactory.Instance.CreateAsync(config,
            new ConfiguredEndpoint(null, endpoint, EndpointConfiguration.Create(config)),
            false, false, config.ApplicationName, 60_000,
            new UserIdentity(_options.Username, _options.Password), null, ct).ConfigureAwait(false);
    }

    private async Task<EndpointDescriptionCollection> GetEndpointsAsync(ApplicationConfiguration config, CancellationToken ct)
    {
        using var discovery = DiscoveryClient.Create(config, new Uri(_options.Url), EndpointConfiguration.Create(config));
        return await discovery.GetEndpointsAsync(null, ct).ConfigureAwait(false);
    }

    /// <summary>Directory stores below the store path; the client certificate is created on first use and then reused.</summary>
    private static async Task ConfigureSecurityAsync(ApplicationConfiguration config, SecurityOptions security, CancellationToken ct)
    {
        var root = security.CertificateStorePath
            ?? System.IO.Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "OpenSmi.Client", "pki");
        config.SecurityConfiguration = new SecurityConfiguration
        {
            ApplicationCertificate = new CertificateIdentifier
            {
                StoreType = CertificateStoreType.Directory,
                StorePath = System.IO.Path.Combine(root, "own"),
                SubjectName = "CN=OpenSMI C# Client",
            },
            TrustedPeerCertificates = new CertificateTrustList
            {
                StoreType = CertificateStoreType.Directory,
                StorePath = security.TrustedPeerStorePath ?? System.IO.Path.Combine(root, "trusted"),
            },
            TrustedIssuerCertificates = new CertificateTrustList
            {
                StoreType = CertificateStoreType.Directory,
                StorePath = System.IO.Path.Combine(root, "issuers"),
            },
            RejectedCertificateStore = new CertificateStoreIdentifier(System.IO.Path.Combine(root, "rejected")),
            AutoAcceptUntrustedCertificates = security.AutoAcceptServerCertificate,
        };
        await config.ValidateAsync(ApplicationType.Client, ct).ConfigureAwait(false);
        await new Opc.Ua.Configuration.ApplicationInstance(config)
            .CheckApplicationInstanceCertificatesAsync(true, null, ct).ConfigureAwait(false);
    }

    private void OnKeepAlive(ISession session, KeepAliveEventArgs e)
    {
        if (!ReferenceEquals(session, Session) || !ServiceResult.IsBad(e.Status)
            || Status is not (RemoteServerStatus.Connected or RemoteServerStatus.Reconnecting)) return;
        e.CancelKeepAlive = true;
        if (_reconnect == null)
        {
            // No reconnect wanted: clean up like a user disconnect, but not on the SDK keep-alive thread.
            if (Status == RemoteServerStatus.Connected) _ = Task.Run(() => DisconnectAsync());
            return;
        }
        _reconnect.BeginReconnect(session, 1000, OnReconnected);
        SetStatusFromSdk(RemoteServerStatus.Reconnecting);
    }

    /// <summary>A recreated session carries cloned subscriptions (with our handlers), so objects stay and only the session is swapped.</summary>
    private void OnReconnected(object? sender, EventArgs e)
    {
        var session = ((SessionReconnectHandler)sender!).Session;
        if (Status != RemoteServerStatus.Reconnecting)
        {
            if (!ReferenceEquals(session, Session)) session?.Dispose(); // disconnect won the race
            return;
        }
        if (session != null && !ReferenceEquals(session, Session))
        {
            var old = Session;
            Session = session;
            Subscriptions.Rebind(session);
            old?.Dispose(); // not CloseAsync: that would delete the subscriptions on the server
        }
        SetStatusFromSdk(RemoteServerStatus.Connected);
    }

    private async Task TearDownAsync()
    {
        _reconnect?.Dispose();
        _reconnect = null;
        try
        {
            if (Subscriptions != null) await Subscriptions.DisposeAsync().ConfigureAwait(false);
            if (Session != null) await Session.CloseAsync(true, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e) { Logger.LogWarning(e, "Error while disconnecting"); }
        finally
        {
            Session?.Dispose();
            Session = null;
            Subscriptions = null!;
            await SetStatusAsync(RemoteServerStatus.Disconnected).ConfigureAwait(false);
        }
    }

    private Task SetStatusAsync(RemoteServerStatus status) => TrySetStatus(status) ? RaiseStatusAsync(status) : Task.CompletedTask;

    /// <summary>From SDK callbacks: the status changes now, handlers run on the notification loop.</summary>
    private void SetStatusFromSdk(RemoteServerStatus status)
    {
        if (TrySetStatus(status)) Subscriptions?.Post(() => RaiseStatusAsync(status));
    }

    private bool TrySetStatus(RemoteServerStatus status)
    {
        if (status == Status) return false;
        Status = status;
        Logger.LogInformation("New status {Status}", status);
        return true;
    }

    private Task RaiseStatusAsync(RemoteServerStatus status)
    {
        _statusWatchers.Publish(status);
        return StatusChanged.RaiseAsync(this, new(status), Logger);
    }
}
