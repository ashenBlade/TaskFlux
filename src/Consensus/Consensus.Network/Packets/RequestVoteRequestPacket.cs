using Consensus.Core.Commands.RequestVote;

namespace Consensus.Network.Packets;

public class RequestVoteRequestPacket: IPacket
{
    public RequestVoteRequest Request { get; }
    public PacketType PacketType => PacketType.RequestVoteRequest;
    public RequestVoteRequestPacket(RequestVoteRequest request)
    {
        Request = request;
    }

    public int EstimatePacketSize()
    {
        return 1  // Маркер
             + 4  // Размер
             + 4  // Candidate Id
             + 4  // Candidate Term
             + 4  // LastLogEntry Term
             + 4; // LastLogEntry Index
    }
}