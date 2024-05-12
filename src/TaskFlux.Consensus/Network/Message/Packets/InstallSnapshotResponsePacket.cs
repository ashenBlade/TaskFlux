using TaskFlux.Consensus.Cluster;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Network.Message.Packets;

public class InstallSnapshotResponsePacket : NodePacket
{
    public Term CurrentTerm { get; }
    public override NodePacketType PacketType => NodePacketType.InstallSnapshotResponse;

    public InstallSnapshotResponsePacket(Term term)
    {
        CurrentTerm = term;
    }

    protected override int EstimatePayloadSize()
    {
        return PayloadSize;
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(CurrentTerm.Value);
    }

    private const int PayloadSize = SizeOf.Term;

    public new static InstallSnapshotResponsePacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[PayloadSize + sizeof(uint)];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    public new static async Task<InstallSnapshotResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        using var buffer = Rent(PayloadSize + sizeof(uint));
        await stream.ReadExactlyAsync(buffer.GetMemory(), token);
        return DeserializePayload(buffer.GetSpan());
    }

    private static InstallSnapshotResponsePacket DeserializePayload(Span<byte> buffer)
    {
        VerifyCheckSum(buffer);
        var reader = new SpanBinaryReader(buffer);
        var term = reader.ReadTerm();
        return new InstallSnapshotResponsePacket(term);
    }
}