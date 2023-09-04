using Consensus.Raft;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Network.Packets;

public class InstallSnapshotResponsePacket : RaftPacket
{
    public Term CurrentTerm { get; }
    public override RaftPacketType PacketType => RaftPacketType.InstallSnapshotResponse;

    public InstallSnapshotResponsePacket(Term term)
    {
        CurrentTerm = term;
    }

    protected override int EstimatePacketSize()
    {
        return sizeof(RaftPacketType) // Маркер
             + sizeof(int);           // Терм
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.InstallSnapshotResponse);
        writer.Write(CurrentTerm.Value);
    }
}