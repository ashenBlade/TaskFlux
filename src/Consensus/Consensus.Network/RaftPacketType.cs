namespace Consensus.Network;

public enum RaftPacketType: byte
{
    ConnectRequest,
    ConnectResponse,
    
    RequestVoteRequest,
    RequestVoteResponse,
    
    AppendEntriesRequest,
    AppendEntriesResponse,
}