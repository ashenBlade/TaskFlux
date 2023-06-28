namespace Raft.Network;

public interface IPacket
{
    public PacketType PacketType { get; }
}