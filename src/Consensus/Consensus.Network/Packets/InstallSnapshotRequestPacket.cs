using Consensus.Raft;
using Consensus.Raft.Persistence;
using TaskFlux.Core;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Network.Packets;

public class InstallSnapshotRequestPacket : RaftPacket
{
    public Term Term { get; }
    public NodeId LeaderId { get; }
    public LogEntryInfo LastEntry { get; }
    public override RaftPacketType PacketType => RaftPacketType.InstallSnapshotRequest;

    public InstallSnapshotRequestPacket(Term term, NodeId leaderId, LogEntryInfo lastEntry)
    {
        Term = term;
        LeaderId = leaderId;
        LastEntry = lastEntry;
    }

    protected override int EstimatePacketSize()
    {
        return sizeof(RaftPacketType) // Маркер
             + sizeof(int)            // Терм
             + sizeof(int)            // Id узла
             + sizeof(uint)           // Последний индекс
             + sizeof(int);           // Последний терм
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.InstallSnapshotRequest);
        writer.Write(Term.Value);
        writer.Write(LeaderId.Id);
        writer.Write(LastEntry.Index);
        writer.Write(LastEntry.Term.Value);
    }
}