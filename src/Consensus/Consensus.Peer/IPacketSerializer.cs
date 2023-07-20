using Consensus.Network;

namespace Consensus.Peer;

public interface IPacketSerializer
{
    public void Serialize(IPacket packet, BinaryWriter writer);
    public IPacket Deserialize(BinaryReader reader);
}