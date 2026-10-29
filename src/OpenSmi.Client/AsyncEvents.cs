using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace OpenSmi.Client;

/// <summary>
/// Handler of the client's events. Handlers run one after another in subscription order on the client's
/// notification loop (never on an SDK thread); an exception is logged and does not stop the next handler.
/// Keep handlers short and never await a future notification of the same client inside one
/// (e.g. <c>StartAsync(StartWait.Completion)</c>): that notification waits for the handler, so it deadlocks
/// until the timeout. Use the <c>Watch…Async</c> streams or the <c>WaitFor…Async</c> methods for that.
/// </summary>
public delegate Task AsyncEventHandler<in TEventArgs>(object sender, TEventArgs e);

public sealed record ServerStatusChangedEventArgs(RemoteServerStatus Status);
public sealed record ServerTimeUpdatedEventArgs(DateTime Time);
public sealed record ComponentStatusChangedEventArgs(string State);
public sealed record NotificationReceivedEventArgs(NotificationEvent Notification);
public sealed record SkillStateChangedEventArgs(SkillState State);
public sealed record VariableValueChangedEventArgs(Variant Value);
public sealed record LockedChangedEventArgs(bool Locked);
public sealed record LockingUserChangedEventArgs(string User);
public sealed record RenewDueEventArgs(double RemainingMs);
public sealed record UserPresenceChangedEventArgs(bool IsPresent);

internal static class AsyncEvents
{
    public static async Task RaiseAsync<T>(this AsyncEventHandler<T>? handlers, object sender, T args, ILogger logger)
    {
        if (handlers == null) return;
        foreach (var handler in handlers.GetInvocationList().Cast<AsyncEventHandler<T>>())
        {
            try { await handler(sender, args).ConfigureAwait(false); }
            catch (Exception e) { logger.LogError(e, "{Event} handler failed", typeof(T).Name); }
        }
    }
}

/// <summary>Backs the <c>Watch…Async</c> streams: one small DropOldest buffer per enumeration.</summary>
internal sealed class Watchers<T>
{
    private const int Capacity = 8;
    private readonly List<ChannelWriter<T>> _writers = [];

    public int Count { get { lock (_writers) return _writers.Count; } }

    public void Publish(T value)
    {
        lock (_writers) foreach (var w in _writers) w.TryWrite(value);
    }

    /// <summary>Yields <paramref name="current"/> first, then every published value until cancelled or disposed.</summary>
    public async IAsyncEnumerable<T> WatchAsync(Func<T> current, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(Capacity)
            { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });
        lock (_writers)
        {
            channel.Writer.TryWrite(current());
            _writers.Add(channel.Writer);
        }
        try
        {
            await foreach (var value in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false)) yield return value;
        }
        finally
        {
            lock (_writers) _writers.Remove(channel.Writer);
        }
    }
}
