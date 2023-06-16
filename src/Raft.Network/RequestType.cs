namespace Raft.Network;

public enum RequestType: byte
{
    Connect,
    RequestVote,
    AppendEntries
}