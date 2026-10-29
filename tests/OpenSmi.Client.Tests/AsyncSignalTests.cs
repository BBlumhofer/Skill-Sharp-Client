namespace OpenSmi.Client.Tests;

public class AsyncSignalTests
{
    [Fact]
    public async Task Failing_handler_reaches_neither_sender_nor_other_handlers()
    {
        var signal = new AsyncSignal<int>();
        var received = 0;
        signal.Connect((_, _) => throw new InvalidOperationException("boom"));
        signal.Connect((v, _) => { received = v; return Task.CompletedTask; });

        await signal.SendAsync(42);

        Assert.Equal(42, received);
    }

    [Fact]
    public async Task Same_handler_is_connected_once_and_can_be_disconnected()
    {
        var signal = new AsyncSignal<int>();
        var calls = 0;
        Func<int, CancellationToken, Task> handler = (_, _) => { Interlocked.Increment(ref calls); return Task.CompletedTask; };
        signal.Connect(handler);
        signal.Connect(handler);
        await signal.SendAsync(1);
        signal.Disconnect(handler);
        await signal.SendAsync(2);

        Assert.Equal(1, calls);
    }
}
