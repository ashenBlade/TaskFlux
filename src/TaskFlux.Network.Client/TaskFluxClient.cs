using System.Buffers;
using System.Net;
using System.Net.Sockets;
using TaskFlux.Network.Requests;
using TaskFlux.Network.Requests.Serialization;

namespace TaskFlux.Network.Client;

public class TaskFluxClient : ITaskFluxClient, IDisposable
{
    private readonly Socket _socket;
    private readonly bool _ownsSocket;

    private readonly Lazy<PoolingNetworkPacketSerializer> _lazySerializer;
    private PoolingNetworkPacketSerializer Serializer => _lazySerializer.Value;

    private TaskFluxClient(Socket socket, bool ownsSocket)
    {
        _socket = socket;
        _ownsSocket = ownsSocket;
        Lazy<NetworkStream> lazyStream = new(() => new NetworkStream(_socket));
        _lazySerializer = new Lazy<PoolingNetworkPacketSerializer>(() =>
            new PoolingNetworkPacketSerializer(ArrayPool<byte>.Shared, lazyStream.Value));
    }

    public TaskFluxClient() : this(new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp), true)
    {
    }

    public TaskFluxClient(Socket socket) : this(ValidateSocket(socket), false)
    {
    }

    private static Socket ValidateSocket(Socket socket)
    {
        ArgumentNullException.ThrowIfNull(socket);
        return socket;
    }

    public Task ConnectAsync(EndPoint endPoint, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        if (token.IsCancellationRequested)
        {
            return Task.FromCanceled(token);
        }

        return ConnectAsyncCore(endPoint, token);
    }

    public Task DisconnectAsync(CancellationToken token = default)
    {
        return _socket.DisconnectAsync(true, token).AsTask();
    }

    private async Task ConnectAsyncCore(EndPoint endPoint, CancellationToken token)
    {
        await _socket.ConnectAsync(endPoint, token);
    }

    public Task SendAsync(Packet packet, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(packet);
        if (token.IsCancellationRequested)
        {
            return Task.FromCanceled(token);
        }

        return packet.AcceptAsync(Serializer, token).AsTask();
    }

    public Task<Packet> ReceiveAsync(CancellationToken token = default)
    {
        if (token.IsCancellationRequested)
        {
            return Task.FromCanceled<Packet>(token);
        }

        return Serializer.DeserializeAsync(token);
    }

    public void Dispose()
    {
        if (_ownsSocket)
        {
            _socket.Dispose();
        }
    }
}