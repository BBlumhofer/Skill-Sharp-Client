using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace OpenSmi.Client;

public sealed record RemoteLockState(bool Locked, DateTime? LockedSince, string LockingUser, string LockingClient,
    int? RemainingLockTimeMs, bool LockedByUs);

/// <summary>
/// Port of <c>client/remote_lock.py</c> (DI LockingServicesType). All properties are cached and updated by
/// subscription, so they are eventually consistent. Ownership is decided only by the user name.
/// </summary>
public sealed class RemoteLock : RemoteUaObject
{
    private const string DiNamespace = "http://opcfoundation.org/UA/DI/";
    private NodeId _locked = null!, _lockingUser = null!, _lockingClient = null!, _remaining = null!;
    private NodeId _initLock = null!, _breakLock = null!, _exitLock = null!, _renewLock = null!;
    private int _maxInactiveLockTimeMs = 60_000;
    private double _renewThreshold = 0.5;
    private TimeSpan _renewRetryInterval = TimeSpan.FromSeconds(1);
    private long? _lastRenewDue;

    internal RemoteLock(NodeId nodeId, RemoteServer server, BaseRemoteComponent parent) : base("Lock", nodeId, server, parent)
    {
        LockedChanged = new(Logger);
        LockingUserChanged = new(Logger);
        RenewDue = new(Logger);
    }

    public bool IsLocked { get; private set; }
    public string LockingUser { get; private set; } = "";
    public string LockingClient { get; private set; } = "";
    public DateTime? LockedSince { get; private set; }
    public bool IsLockedByUs => IsLocked && LockingUser == Server.Username;
    /// <summary><c>null</c> while unlocked.</summary>
    public int? RemainingLockTimeMs { get; private set; }

    public int MaxInactiveLockTimeMs
    {
        get => _maxInactiveLockTimeMs;
        set => _maxInactiveLockTimeMs = value > 0 ? value : throw new ArgumentOutOfRangeException(nameof(value), "must be > 0");
    }

    /// <summary>Fraction of <see cref="MaxInactiveLockTimeMs"/> below which <see cref="RenewDue"/> fires.</summary>
    public double RenewThreshold
    {
        get => _renewThreshold;
        set => _renewThreshold = value is > 0 and < 1 ? value : throw new ArgumentOutOfRangeException(nameof(value), "must be in (0, 1)");
    }

    /// <summary>Minimum time between two <see cref="RenewDue"/> signals.</summary>
    public TimeSpan RenewRetryInterval
    {
        get => _renewRetryInterval;
        set => _renewRetryInterval = value > TimeSpan.Zero ? value : throw new ArgumentOutOfRangeException(nameof(value), "must be > 0");
    }

    public AsyncSignal<(RemoteLock Sender, bool Locked)> LockedChanged { get; }
    public AsyncSignal<(RemoteLock Sender, string User)> LockingUserChanged { get; }
    /// <summary>Payload: remaining lock time in ms.</summary>
    public AsyncSignal<(RemoteLock Sender, double RemainingMs)> RenewDue { get; }

    public Task<LockResult> InitLockAsync(CancellationToken cancellationToken = default) => CallAsync(_initLock, cancellationToken);
    public Task<LockResult> BreakLockAsync(CancellationToken cancellationToken = default) => CallAsync(_breakLock, cancellationToken);
    public Task<LockResult> ExitLockAsync(CancellationToken cancellationToken = default) => CallAsync(_exitLock, cancellationToken);
    public Task<LockResult> RenewLockAsync(CancellationToken cancellationToken = default) => CallAsync(_renewLock, cancellationToken);

    /// <summary>Reads the four lock variables in one request.</summary>
    public async Task<RemoteLockState> ReadStateAsync(bool updateCache = true, CancellationToken cancellationToken = default)
    {
        var values = await UaUtil.ReadAsync(Session,
            new[] { _locked, _lockingUser, _lockingClient, _remaining }.Select(n => (n, Opc.Ua.Attributes.Value)), cancellationToken)
            .ConfigureAwait(false);
        foreach (var v in values)
        {
            OpenSmiStatusException.ThrowIfBad(v.StatusCode, $"Read of {Path}");
            if (v.Value == null) throw new OpenSmiProtocolException($"{Path}: lock variable without value");
        }
        if (updateCache)
        {
            await OnLockedAsync(values[0]).ConfigureAwait(false);
            await OnLockingUserAsync(values[1]).ConfigureAwait(false);
            await OnLockingClientAsync(values[2]).ConfigureAwait(false);
            await OnRemainingAsync(values[3]).ConfigureAwait(false);
        }
        var locked = (bool)values[0].Value;
        return new RemoteLockState(locked, locked ? Timestamp(values[0]) : null,
            locked ? (string)values[1].Value : "", locked ? (string)values[2].Value : "",
            locked ? Convert.ToInt32(values[3].Value) : null, locked && (string)values[1].Value == Server.Username);
    }

    private protected override async Task InitCoreAsync(CancellationToken ct)
    {
        var children = await UaUtil.BrowseChildrenAsync(Session, NodeId, ct).ConfigureAwait(false);
        NodeId Required(string name) => children.Child(name) is { } c ? UaUtil.ToNodeId(Session, c.NodeId)
            : throw new OpenSmiProtocolException($"{Path}: lock has no '{name}'");
        (_locked, _lockingUser, _lockingClient, _remaining) =
            (Required("Locked"), Required("LockingUser"), Required("LockingClient"), Required("RemainingLockTime"));
        (_initLock, _breakLock, _exitLock, _renewLock) =
            (Required("InitLock"), Required("BreakLock"), Required("ExitLock"), Required("RenewLock"));

        try
        {
            var max = (await UaUtil.ReadAsync(Session, [(new NodeId(6387u, Server.GetNamespaceIndex(DiNamespace)),
                Opc.Ua.Attributes.Value)], ct).ConfigureAwait(false))[0];
            if (StatusCode.IsGood(max.StatusCode) && max.Value != null) MaxInactiveLockTimeMs = Convert.ToInt32(max.Value);
        }
        catch (Exception e) when (e is KeyNotFoundException or ServiceResultException or ArgumentOutOfRangeException)
        {
            Logger.LogWarning(e, "Could not read MaxInactiveLockTime, using {Ms} ms", MaxInactiveLockTimeMs);
        }

        await Server.Subscriptions.SubscribeDataChangeAsync([(_locked, OnLockedAsync), (_lockingUser, OnLockingUserAsync),
            (_lockingClient, OnLockingClientAsync), (_remaining, OnRemainingAsync)], ct).ConfigureAwait(false);
    }

    private async Task<LockResult> CallAsync(NodeId method, CancellationToken ct)
    {
        var output = await UaUtil.CallAsync(Session, NodeId, method, Path, ct).ConfigureAwait(false);
        return output.Count > 0 && output[0].Value != null ? (LockResult)Convert.ToInt32(output[0].Value) : LockResult.Ok;
    }

    private Task OnLockedAsync(DataValue dv)
    {
        var locked = dv.Value is true;
        if (locked == IsLocked) return Task.CompletedTask;
        IsLocked = locked;
        LockedSince = locked ? Timestamp(dv) : null;
        if (!locked) RemainingLockTimeMs = null;
        return LockedChanged.SendAsync((this, locked));
    }

    private Task OnLockingUserAsync(DataValue dv)
    {
        var user = dv.Value as string ?? "";
        if (user == LockingUser) return Task.CompletedTask;
        LockingUser = user;
        return LockingUserChanged.SendAsync((this, user));
    }

    private Task OnLockingClientAsync(DataValue dv)
    {
        LockingClient = dv.Value as string ?? "";
        return Task.CompletedTask;
    }

    private Task OnRemainingAsync(DataValue dv)
    {
        RemainingLockTimeMs = IsLocked && dv.Value != null ? Convert.ToInt32(dv.Value) : null;
        return CheckRenewDueAsync();
    }

    private Task CheckRenewDueAsync()
    {
        if (!IsLockedByUs || RemainingLockTimeMs is not { } remaining) return Task.CompletedTask;
        if (remaining > MaxInactiveLockTimeMs) MaxInactiveLockTimeMs = remaining;
        if (remaining >= MaxInactiveLockTimeMs * RenewThreshold)
        {
            _lastRenewDue = null; // re-arm
            return Task.CompletedTask;
        }
        var now = Stopwatch.GetTimestamp();
        if (_lastRenewDue is { } last && Stopwatch.GetElapsedTime(last, now) < RenewRetryInterval) return Task.CompletedTask;
        _lastRenewDue = now;
        return RenewDue.SendAsync((this, remaining));
    }

    private static DateTime Timestamp(DataValue dv) => dv.ServerTimestamp == DateTime.MinValue ? DateTime.UtcNow : dv.ServerTimestamp;
}

/// <summary>
/// Port of <c>client/auto_lock.py</c>: keeps the lock, re-locks when another user takes or releases it and
/// renews it when <see cref="RemoteLock.RenewDue"/> fires.
/// </summary>
public sealed class AutoLock(RemoteLock remoteLock, bool breakLock = false, TimeSpan? interval = null) : IAsyncDisposable
{
    private readonly TimeSpan _interval = interval ?? TimeSpan.FromSeconds(1);
    private readonly ILogger _logger = remoteLock.Server.LoggerFactory.CreateLogger<AutoLock>();
    private CancellationTokenSource? _active;
    private Task? _renew;

    public bool IsActive => _active != null;

    /// <summary>With <paramref name="blocking"/> it returns once the lock is ours; otherwise it locks in the background.</summary>
    public async Task LockAsync(bool blocking = true, CancellationToken cancellationToken = default)
    {
        if (_active == null)
        {
            _active = new CancellationTokenSource();
            remoteLock.LockingUserChanged.Connect(OnLockingUserChangedAsync);
            remoteLock.RenewDue.Connect(OnRenewDueAsync);
        }
        if (blocking) await LockOnceAsync(cancellationToken).ConfigureAwait(false);
        else _ = LockOnceAsync(CancellationToken.None);
    }

    public async Task UnlockAsync(CancellationToken cancellationToken = default)
    {
        if (_active == null) return;
        remoteLock.LockingUserChanged.Disconnect(OnLockingUserChangedAsync);
        remoteLock.RenewDue.Disconnect(OnRenewDueAsync);
        await _active.CancelAsync().ConfigureAwait(false);
        _active.Dispose();
        _active = null;
        if (!remoteLock.IsLockedByUs) return;
        try { await remoteLock.ExitLockAsync(cancellationToken).ConfigureAwait(false); }
        catch (OpenSmiException e) { _logger.LogWarning(e, "ExitLock failed"); }
    }

    public ValueTask DisposeAsync() => new(UnlockAsync());

    private async Task LockOnceAsync(CancellationToken ct)
    {
        var active = _active;
        if (active == null) return;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, active.Token);
        try
        {
            while (!remoteLock.IsLockedByUs)
            {
                try
                {
                    await (breakLock ? remoteLock.BreakLockAsync(cts.Token) : remoteLock.InitLockAsync(cts.Token)).ConfigureAwait(false);
                    break; // the cached state lags behind the call
                }
                catch (OpenSmiException e)
                {
                    _logger.LogInformation("Locking {Path} failed ({Reason}), retrying", remoteLock.Path, e.Message);
                    await Task.Delay(_interval, cts.Token).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested) { } // UnlockAsync stopped us
    }

    private Task OnLockingUserChangedAsync((RemoteLock, string User) args, CancellationToken ct)
    {
        // Not awaited: the handler runs on the subscription dispatch loop, which delivers the lock updates.
        if (args.User != remoteLock.Server.Username) _ = LockOnceAsync(CancellationToken.None);
        return Task.CompletedTask;
    }

    private Task OnRenewDueAsync((RemoteLock, double) _, CancellationToken __)
    {
        if (_renew is { IsCompleted: false }) return Task.CompletedTask;
        _renew = Task.Run(async () =>
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try { await remoteLock.RenewLockAsync(timeout.Token).ConfigureAwait(false); }
            catch (Exception e) when (e is OpenSmiException or OperationCanceledException) { _logger.LogWarning(e, "RenewLock failed"); }
        });
        return Task.CompletedTask;
    }
}
