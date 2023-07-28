using System.Net;
using System.Net.Sockets;
using Consensus.Network;

namespace Consensus.Peer;

public class PacketClient: IDisposable
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

    private async ValueTask SendCore(RaftPacket packet, CancellationToken token)
    {
        await packet.Serialize(Stream, token);
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

    private static bool IsNetworkError(SocketError error)
    {
        return error is 
                   SocketError.Shutdown or 
                   SocketError.NotConnected or 
                   SocketError.Disconnecting or 
                   
                   SocketError.ConnectionReset or 
                   SocketError.ConnectionAborted or 
                   SocketError.ConnectionRefused or 
                   
                   SocketError.NetworkDown or 
                   SocketError.NetworkReset or 
                   SocketError.NetworkUnreachable or 
                   
                   SocketError.HostDown or 
                   SocketError.HostUnreachable or 
                   SocketError.HostNotFound;
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
        catch (IOException io) 
            when (io.GetBaseException() is SocketException)
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
}