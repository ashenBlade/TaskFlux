namespace TaskFlux.Consensus.Network.Message.Packets;

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

    public new static InstallSnapshotChunkResponsePacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<InstallSnapshotChunkResponsePacket> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        using var buffer = Rent(sizeof(uint));
        await stream.ReadExactlyAsync(buffer.GetMemory(), token);
        return DeserializePayload(buffer.GetSpan());
    }

    private static InstallSnapshotChunkResponsePacket DeserializePayload(Span<byte> payload)
    {
        VerifyCheckSum(payload);
        return new InstallSnapshotChunkResponsePacket();
    }
}