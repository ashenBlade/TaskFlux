using Raft.Core;

namespace Raft.Network.Packets;

public class ConnectRequestPacket: IPacket
{
    public PacketType PacketType => PacketType.ConnectRequest;
    public NodeId Id { get; }

    public ConnectRequestPacket(NodeId id)
    {
        Id = id;
    }
}