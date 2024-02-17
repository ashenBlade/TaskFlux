using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster.Network.Packets;

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

    protected override int EstimatePayloadSize()
    {
        return PayloadSize;
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(Term.Value);
        writer.Write(LeaderId.Id);
        writer.Write(LastEntry.Index);
        writer.Write(LastEntry.Term.Value);
    }

    private const int PayloadSize = SizeOf.Term   // Терм лидера
                                  + SizeOf.NodeId // Id лидера
                                  + SizeOf.Lsn    // Последний индекс лидера 
                                  + SizeOf.Term;  // Последний терм лидера

    public new static InstallSnapshotRequestPacket Deserialize(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[PayloadSize + sizeof(uint)];
        stream.ReadExactly(buffer);

        return DeserializePayload(buffer);
    }

    private static InstallSnapshotRequestPacket DeserializePayload(Span<byte> buffer)
    {
        VerifyCheckSum(buffer);
        var reader = new SpanBinaryReader(buffer);
        var term = reader.ReadTerm();
        var leaderId = reader.ReadNodeId();
        var lastIndex = reader.ReadLsn();
        var lastTerm = reader.ReadTerm();
        return new InstallSnapshotRequestPacket(term, leaderId,
            new LogEntryInfo(lastTerm, lastIndex));
    }

    public new static async Task<InstallSnapshotRequestPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        using var buffer = Rent(PayloadSize + sizeof(uint));
        await stream.ReadExactlyAsync(buffer.GetMemory(), token);
        return DeserializePayload(buffer.GetSpan());
    }
}