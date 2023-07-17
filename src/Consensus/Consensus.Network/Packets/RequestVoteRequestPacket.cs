using Consensus.Core.Commands.RequestVote;

namespace Consensus.Network.Packets;

public class RequestVoteRequestPacket: IPacket
{
    public RequestVoteRequestPacket(RequestVoteRequest request)
    {
        Request = request;
    }

    public PacketType PacketType => PacketType.RequestVoteRequest;
    public RequestVoteRequest Request { get; }
}