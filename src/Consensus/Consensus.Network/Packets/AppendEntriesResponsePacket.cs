using Consensus.Raft.Commands.AppendEntries;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Network.Packets;

public class AppendEntriesResponsePacket : RaftPacket
{
    public override RaftPacketType PacketType => RaftPacketType.AppendEntriesResponse;

    protected override int EstimatePacketSize()
    {
        return 1  // Маркер
             + 1  // Success
             + 4; // Term
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.AppendEntriesResponse);
        writer.Write(Response.Success);
        writer.Write(Response.Term.Value);
    }


    public AppendEntriesResponse Response { get; }

    public AppendEntriesResponsePacket(AppendEntriesResponse response)
    {
        Response = response;
    }
}