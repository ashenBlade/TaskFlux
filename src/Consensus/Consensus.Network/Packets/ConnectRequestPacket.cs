using System.Buffers;
using Consensus.Core;

namespace Consensus.Network.Packets;

public class ConnectRequestPacket: IPacket
{
    public PacketType PacketType => PacketType.ConnectRequest;
    public int EstimatePacketSize()
    {
        return 1  // Маркер
             + 4  // Длина
             + 4; // NodeId
    }
    public NodeId Id { get; }

    public ConnectRequestPacket(NodeId id)
    {
        Id = id;
    }
}