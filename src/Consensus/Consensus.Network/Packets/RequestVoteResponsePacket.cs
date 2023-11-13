using Consensus.Raft.Commands.RequestVote;
using Utils.Serialization;

namespace Consensus.Network.Packets;

public class RequestVoteResponsePacket : RaftPacket
{
    public RequestVoteResponse Response { get; }

    public RequestVoteResponsePacket(RequestVoteResponse response)
    {
        Response = response;
    }

    public override RaftPacketType PacketType => RaftPacketType.RequestVoteResponse;

    protected override int EstimatePacketSize()
    {
        return 1  // Маркер
             + 1  // Vote Granted
             + 4; // Current Term
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.RequestVoteResponse);
        writer.Write(Response.VoteGranted);
        writer.Write(Response.CurrentTerm.Value);
    }
}