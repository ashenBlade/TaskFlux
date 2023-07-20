using Consensus.Core.Commands.RequestVote;

namespace Consensus.Network.Packets;

public class RequestVoteResponsePacket: IPacket
{
    public RequestVoteResponse Response { get; }
    public RequestVoteResponsePacket(RequestVoteResponse response)
    {
        Response = response;
    }

    public PacketType PacketType => PacketType.RequestVoteResponse;
    public int EstimatePacketSize()
    {
        return 1  // Маркер
             + 4  // Размер
             + 1  // Vote Granted
             + 4; // Current Term
    }
}