namespace Consensus.Network;

public interface IPacket
{
    public PacketType PacketType { get; }
}