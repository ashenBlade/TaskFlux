using TaskFlux.Consensus.Persistence;
using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Cluster.Network.Packets;

public sealed class InstallSnapshotRequestPacket : NodePacket
{
    public Term Term { get; }
    public NodeId LeaderId { get; }
    public LogEntryInfo LastEntry { get; }
    public override NodePacketType PacketType => NodePacketType.InstallSnapshotRequest;

    public InstallSnapshotRequestPacket(Term term, NodeId leaderId, LogEntryInfo lastEntry)
    {
        Term = term;
        LeaderId = leaderId;
        LastEntry = lastEntry;
    }

    protected override int EstimatePacketSize()
    {
        return sizeof(NodePacketType) // Маркер
             + sizeof(int)            // Терм
             + sizeof(int)            // Id узла
             + sizeof(uint)           // Последний индекс
             + sizeof(int);           // Последний терм
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) NodePacketType.InstallSnapshotRequest);
        writer.Write(Term.Value);
        writer.Write(LeaderId.Id);
        writer.Write(LastEntry.Index);
        writer.Write(LastEntry.Term.Value);
    }

    public new static InstallSnapshotRequestPacket Deserialize(Stream stream)
    {
        const int packetSize = sizeof(int)  // Терм
                             + sizeof(int)  // Id лидера
                             + sizeof(int)  // Последний индекс
                             + sizeof(int); // Последний терм
        Span<byte> buffer = stackalloc byte[packetSize];
        stream.ReadExactly(buffer);

        return DeserializePayload(buffer);
    }

    private static InstallSnapshotRequestPacket DeserializePayload(Span<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer);
        var term = reader.ReadInt32();
        var leaderId = reader.ReadInt32();
        var lastIndex = reader.ReadInt32();
        var lastTerm = reader.ReadInt32();
        return new InstallSnapshotRequestPacket(new Term(term), new NodeId(leaderId),
            new LogEntryInfo(new Term(lastTerm), lastIndex));
    }

    public new static async Task<InstallSnapshotRequestPacket> DeserializeAsync(
        Stream stream,
        CancellationToken token)
    {
        const int packetSize = sizeof(int)  // Терм
                             + sizeof(int)  // Id лидера
                             + sizeof(int)  // Последний индекс
                             + sizeof(int); // Последний терм
        using var buffer = Rent(packetSize);
        await stream.ReadExactlyAsync(buffer.GetMemory(), token);

        return DeserializePayload(buffer.GetSpan());
    }
}