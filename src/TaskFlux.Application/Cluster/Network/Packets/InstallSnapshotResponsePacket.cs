using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Cluster.Network.Packets;

public class InstallSnapshotResponsePacket : NodePacket
{
    public Term CurrentTerm { get; }
    public override NodePacketType PacketType => NodePacketType.InstallSnapshotResponse;

    public InstallSnapshotResponsePacket(Term term)
    {
        CurrentTerm = term;
    }

    protected override int EstimatePacketSize()
    {
        return sizeof(NodePacketType) // Маркер
             + sizeof(int);           // Терм
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) NodePacketType.InstallSnapshotResponse);
        writer.Write(CurrentTerm.Value);
    }

    public new static InstallSnapshotResponsePacket Deserialize(Stream stream)
    {
        const int packetSize = sizeof(int); // Терм
        Span<byte> buffer = stackalloc byte[packetSize];
        stream.ReadExactly(buffer);
        return DeserializePayload(buffer);
    }

    private static InstallSnapshotResponsePacket DeserializePayload(Span<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer);
        var term = reader.ReadInt32();
        return new InstallSnapshotResponsePacket(new Term(term));
    }

    public new static async Task<InstallSnapshotResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        const int packetSize = sizeof(int); // Терм
        using var buffer = Rent(packetSize);
        await stream.ReadExactlyAsync(buffer.GetMemory(), token);
        return DeserializePayload(buffer.GetSpan());
    }
}