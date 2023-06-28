using Raft.Core.Commands.RequestVote;

namespace Raft.Network.Packets;

public class RequestVoteRequestPacket: IPacket
{
    public RequestVoteRequestPacket(RequestVoteRequest request)
    {
        Request = request;
    }

    public PacketType PacketType => PacketType.RequestVoteRequest;
    public RequestVoteRequest Request { get; }
}