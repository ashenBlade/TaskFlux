using Consensus.Raft.Commands.RequestVote;
using Utils.Serialization;

namespace Consensus.Network.Packets;

public class RequestVoteRequestPacket : RaftPacket
{
    public RequestVoteRequest Request { get; }
    public override RaftPacketType PacketType => RaftPacketType.RequestVoteRequest;

    public RequestVoteRequestPacket(RequestVoteRequest request)
    {
        Request = request;
    }

    protected override int EstimatePacketSize()
    {
        return 1  // Маркер
             + 4  // Candidate Id
             + 4  // Candidate Term
             + 4  // LastLogEntry Term
             + 4; // LastLogEntry Index
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) RaftPacketType.RequestVoteRequest);
        writer.Write(Request.CandidateId.Id);
        writer.Write(Request.CandidateTerm.Value);
        writer.Write(Request.LastLogEntryInfo.Term.Value);
        writer.Write(Request.LastLogEntryInfo.Index);
    }
}