using Raft.Core.Commands.RequestVote;

namespace Raft.Network.Packets;

public class RequestVoteResponsePacket: IPacket
{
    public RequestVoteResponsePacket(RequestVoteResponse response)
    {
        Response = response;
    }

    public PacketType PacketType => PacketType.RequestVoteResponse;
    public RequestVoteResponse Response { get; }
}