using System.Net;
using System.Net.Sockets;

namespace OpenSmi.Client.Tests;

/// <summary>Forwards localhost:<c>port</c> to localhost:<c>target</c>; <see cref="Cut"/> simulates a network drop.</summary>
public sealed class TcpProxy : IDisposable
{
    private readonly int _port, _target;
    private readonly List<TcpClient> _connections = [];
    private TcpListener _listener = null!;

    public TcpProxy(int port, int target)
    {
        (_port, _target) = (port, target);
        Restore();
    }

    /// <summary>Drops every open connection and refuses new ones until <see cref="Restore"/>.</summary>
    public void Cut()
    {
        _listener.Stop();
        lock (_connections)
        {
            foreach (var c in _connections) c.Dispose();
            _connections.Clear();
        }
    }

    public void Restore()
    {
        _listener = new TcpListener(IPAddress.Loopback, _port);
        _listener.Start();
        _ = AcceptAsync(_listener);
    }

    private async Task AcceptAsync(TcpListener listener)
    {
        try
        {
            while (true)
            {
                var inbound = await listener.AcceptTcpClientAsync();
                var outbound = new TcpClient();
                await outbound.ConnectAsync(IPAddress.Loopback, _target);
                lock (_connections) _connections.AddRange([inbound, outbound]);
                _ = PipeAsync(inbound, outbound);
                _ = PipeAsync(outbound, inbound);
            }
        }
        catch (Exception e) when (e is SocketException or ObjectDisposedException) { } // listener stopped
    }

    private static async Task PipeAsync(TcpClient from, TcpClient to)
    {
        try { await from.GetStream().CopyToAsync(to.GetStream()); }
        catch (Exception e) when (e is IOException or ObjectDisposedException or InvalidOperationException) { }
        from.Dispose();
        to.Dispose();
    }

    public void Dispose() => Cut();
}
