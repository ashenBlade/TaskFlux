namespace Raft.Network;

public enum PacketType: byte
{
    ConnectRequest,
    ConnectResponse,
    
    RequestVoteRequest,
    RequestVoteResponse,
    
    AppendEntriesRequest,
    AppendEntriesResponse,
}