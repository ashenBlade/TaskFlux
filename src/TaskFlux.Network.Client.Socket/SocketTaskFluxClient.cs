using System.Buffers;
using System.Net.Sockets;
using TaskFlux.Network.Requests;
using TaskFlux.Network.Requests.Serialization;

namespace TaskFlux.Network.Client.Socket;

public class SocketTaskFluxClient: ITaskFluxClient
{
    private NetworkStream Stream { get; }
    private readonly System.Net.Sockets.Socket _socket;
    private readonly PoolingNetworkPacketSerializer _serializer;

    public SocketTaskFluxClient(
        System.Net.Sockets.Socket socket)
    {
        Stream = new(socket);
        _socket = socket;
        _serializer = new PoolingNetworkPacketSerializer(ArrayPool<byte>.Shared);
    }


    public async ValueTask SendAsync(Packet packet, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        using var pooled = _serializer.Serialize(packet);
        await Stream.WriteAsync(pooled.Array.AsMemory(0, pooled.Length), token);
    }

    public async ValueTask<Packet> ReceiveAsync(CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        return await _serializer.DeserializeAsync(Stream, token);
    }
}