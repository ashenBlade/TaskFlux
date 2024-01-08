namespace Consensus.Network.Packets;

public class InstallSnapshotChunkResponsePacket : NodePacket
{
    public override NodePacketType PacketType => NodePacketType.InstallSnapshotChunkResponse;

    protected override int EstimatePacketSize()
    {
        return sizeof(NodePacketType);
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        buffer[0] = ( byte ) NodePacketType.InstallSnapshotChunkResponse;
    }
}