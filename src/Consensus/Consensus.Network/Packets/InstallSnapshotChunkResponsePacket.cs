namespace Consensus.Network.Packets;

public class InstallSnapshotChunkResponsePacket : RaftPacket
{
    public override RaftPacketType PacketType => RaftPacketType.InstallSnapshotChunkResponse;

    protected override int EstimatePacketSize()
    {
        return sizeof(RaftPacketType);
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        buffer[0] = ( byte ) RaftPacketType.InstallSnapshotChunkResponse;
    }
}