namespace TaskFlux.Application.Cluster.Network.Packets;

public class InstallSnapshotChunkResponsePacket : NodePacket
{
    public override NodePacketType PacketType => NodePacketType.InstallSnapshotChunkResponse;

    protected override int EstimatePayloadSize()
    {
        return 0;
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
    }
}