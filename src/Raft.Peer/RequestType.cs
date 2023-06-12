namespace Raft.Peer;

public enum RequestType: byte
{
    Connect,
    RequestVote,
    AppendEntries
}