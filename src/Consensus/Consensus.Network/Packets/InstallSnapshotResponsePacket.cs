using Consensus.Raft.Commands.InstallSnapshot;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Network.Packets;

public class InstallSnapshotResponsePacket : RaftPacket
{
    public InstallSnapshotResponse Response { get; }
    public override RaftPacketType PacketType => RaftPacketType.InstallSnapshotResponse;

    public InstallSnapshotResponsePacket(InstallSnapshotResponse response)
    {
        Response = response;
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
        writer.Write(Response.CurrentTerm.Value);
    }
}