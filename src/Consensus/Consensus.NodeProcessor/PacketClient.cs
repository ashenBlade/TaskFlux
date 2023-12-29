using System.Net.Sockets;
using Consensus.Network;
using Consensus.Peer;

namespace Consensus.NodeProcessor;

public class PacketClient
{
    private static BinaryPacketDeserializer Deserializer => BinaryPacketDeserializer.Instance;
    public Socket Socket { get; }
    private readonly Lazy<NetworkStream> _lazy;
    private NetworkStream Stream => _lazy.Value;

    public PacketClient(Socket socket)
    {
        Socket = socket;
        _lazy = new Lazy<NetworkStream>(() => new NetworkStream(socket));
    }

    public async ValueTask<RaftPacket?> ReceiveAsync(CancellationToken token)
    {
        try
        {
            return await Deserializer.DeserializeAsync(Stream, token);
        }
        catch (SocketException)
        {
        }
        catch (IOException)
        {
        }

        return null;
    }

    public RaftPacket? Receive()
    {
        try
        {
            return Deserializer.Deserialize(Stream);
        }
        catch (SocketException)
        {
        }
        catch (IOException)
        {
        }

        return null;
    }

    public ValueTask SendAsync(RaftPacket packet, CancellationToken token)
    {
        return packet.SerializeAsync(Stream, token);
    }

    public void Send(RaftPacket packet)
    {
        packet.Serialize(Stream);
    }
}