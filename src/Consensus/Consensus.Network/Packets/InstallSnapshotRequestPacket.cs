using Consensus.Raft.Commands.InstallSnapshot;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Network.Packets;

public class InstallSnapshotRequestPacket : RaftPacket
{
    private readonly InstallSnapshotRequest _request;
    public override RaftPacketType PacketType => RaftPacketType.InstallSnapshotRequest;

    public InstallSnapshotRequestPacket(InstallSnapshotRequest request)
    {
        _request = request;
    }

    protected override int EstimatePacketSize()
    {
        return sizeof(RaftPacketType) // Маркер
             + sizeof(int)            // Id узла
             + sizeof(uint)           // Последний индекс
             + sizeof(int);           // Последний терм
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.InstallSnapshotRequest);
        writer.Write(_request.LeaderId.Id);
        writer.Write(_request.LastIncludedIndex);
        writer.Write(_request.LastIncludedTerm.Value);
    }
}