using System.Buffers;
using System.Net;
using System.Net.Sockets;
using TaskFlux.Network.Requests;
using TaskFlux.Network.Requests.Serialization;

namespace TaskFlux.Network.Client;

public class TaskFluxClient: ITaskFluxClient, IDisposable
{
    private readonly Socket _socket;
    private readonly bool _ownsSocket;

    private readonly Lazy<PoolingNetworkPacketSerializer> _lazySerializer;
    private readonly Lazy<NetworkStream> _lazyStream;
    private PoolingNetworkPacketSerializer Serializer => _lazySerializer.Value;

    private TaskFluxClient(Socket socket, bool ownsSocket)
    {
        _socket = socket;
        _ownsSocket = ownsSocket;
        _lazyStream = new Lazy<NetworkStream>(() => new NetworkStream(_socket));
        _lazySerializer = new Lazy<PoolingNetworkPacketSerializer>(() =>
            new PoolingNetworkPacketSerializer(ArrayPool<byte>.Shared, _lazyStream.Value));
    }
    
    public TaskFluxClient(): this(new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp), true)
    { }
    
    public TaskFluxClient(Socket socket): this(ValidateSocket(socket), false)
    { }

    private static Socket ValidateSocket(Socket socket)
    {
        ArgumentNullException.ThrowIfNull(socket);
        return socket;
    }

    public ValueTask ConnectAsync(EndPoint endPoint, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        if (token.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(token);
        }

        return ConnectAsyncCore(endPoint, token);
    }

    public ValueTask DisconnectAsync(CancellationToken token = default)
    {
        return _socket.DisconnectAsync(true, token);
    }

    private async ValueTask ConnectAsyncCore(EndPoint endPoint, CancellationToken token)
    {
        await _socket.ConnectAsync(endPoint, token);
    }

    public ValueTask SendAsync(Packet packet, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(packet);
        if (token.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(token);
        }
        return packet.AcceptAsync(Serializer, token);
    }

    public ValueTask<Packet> ReceiveAsync(CancellationToken token = default)
    {
        if (token.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<Packet>(token);
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