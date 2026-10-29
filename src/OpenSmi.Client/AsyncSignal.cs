using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace OpenSmi.Client;

/// <summary>
/// Port of opensmi-core <c>Signal</c>: handlers run concurrently; a failing handler is logged and
/// never reaches the sender (or the other handlers).
/// </summary>
public sealed class AsyncSignal<T>(ILogger? logger = null)
{
    private readonly ILogger _logger = logger ?? NullLogger.Instance;
    private readonly Lock _gate = new();
    private Func<T, CancellationToken, Task>[] _handlers = [];

    public void Connect(Func<T, CancellationToken, Task> handler)
    {
        lock (_gate) if (!_handlers.Contains(handler)) _handlers = [.. _handlers, handler];
    }

    public void Disconnect(Func<T, CancellationToken, Task> handler)
    {
        lock (_gate) _handlers = [.. _handlers.Where(h => h != handler)];
    }

    public Task SendAsync(T args, CancellationToken cancellationToken = default) =>
        Task.WhenAll(Volatile.Read(ref _handlers).Select(async h =>
        {
            try { await h(args, cancellationToken).ConfigureAwait(false); }
            catch (Exception e) { _logger.LogError(e, "Signal handler failed"); }
        }));
}
