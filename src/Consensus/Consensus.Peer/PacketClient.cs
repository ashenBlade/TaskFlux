using System.Net;
using System.Net.Sockets;
using Consensus.Network;

namespace Consensus.Peer;

public class PacketClient : IDisposable
{
    private readonly BinaryPacketDeserializer _deserializer = BinaryPacketDeserializer.Instance;
    public Socket Socket { get; }
    private readonly Lazy<NetworkStream> _lazyStream;
    private NetworkStream Stream => _lazyStream.Value;

    public PacketClient(Socket socket)
    {
        Socket = socket;
        _lazyStream = new Lazy<NetworkStream>(() => new NetworkStream(socket));
    }

    public ValueTask SendAsync(RaftPacket requestPacket, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        return SendCore(requestPacket, token);
    }

    public void Send(RaftPacket packet, CancellationToken token)
    {
        packet.Serialize(Stream, token);
    }

    private async ValueTask SendCore(RaftPacket packet, CancellationToken token)
    {
        await packet.SerializeAsync(Stream, token);
    }

    public async ValueTask<RaftPacket> ReceiveAsync(CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        return await ReceiveCoreAsync(token);
    }

    private async ValueTask<RaftPacket> ReceiveCoreAsync(CancellationToken token)
    {
        return await _deserializer.DeserializeAsync(Stream, token);
    }

    public async ValueTask<bool> ConnectAsync(EndPoint endPoint, TimeSpan timeout, CancellationToken token = default)
    {
        if (Socket.Connected)
        {
            return true;
        }

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            cts.CancelAfter(timeout);
            await Socket.ConnectAsync(endPoint, cts.Token);
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
        catch (IOException)
        {
            return false;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    public async ValueTask DisconnectAsync(CancellationToken token = default)
    {
        if (Socket.Connected)
        {
            await Socket.DisconnectAsync(true, token);
        }
    }

    public void Dispose()
    {
        if (_lazyStream.IsValueCreated)
        {
            _lazyStream.Value.Flush();
        }
    }

    public RaftPacket Receive(CancellationToken token = default)
    {
        return _deserializer.Deserialize(Stream, token);
    }

    public bool Connect(EndPoint endPoint)
    {
        try
        {
            Socket.Connect(endPoint);
        }
        catch (SocketException)
        {
            return false;
        }
        catch (IOException)
        {
            return false;
        }

        return true;
    }
}