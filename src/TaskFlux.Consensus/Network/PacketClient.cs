using System.Net.Sockets;
using TaskFlux.Consensus.Network.Message;

namespace TaskFlux.Application.Cluster;

public class PacketClient
{
    public Socket Socket { get; }
    private readonly Lazy<NetworkStream> _lazy;
    private NetworkStream Stream => _lazy.Value;

    public PacketClient(Socket socket)
    {
        Socket = socket;
        _lazy = new Lazy<NetworkStream>(() => new NetworkStream(socket));
    }

    public async ValueTask<NodePacket?> ReceiveAsync(CancellationToken token)
    {
        try
        {
            return await NodePacket.DeserializeAsync(Stream, token);
        }
        catch (SocketException)
        {
        }
        catch (IOException)
        {
        }

        return null;
    }

    public NodePacket? Receive()
    {
        try
        {
            return NodePacket.Deserialize(Stream);
        }
        catch (SocketException)
        {
        }
        catch (IOException)
        {
        }

        return null;
    }

    public ValueTask SendAsync(NodePacket packet, CancellationToken token)
    {
        return packet.SerializeAsync(Stream, token);
    }

    public void Send(NodePacket packet)
    {
        packet.Serialize(Stream);
    }
}