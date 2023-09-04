using Consensus.Network;
using Consensus.Network.Packets;

namespace Consensus.Peer.Tests;

public class PacketEqualityComparer : IEqualityComparer<RaftPacket>
{
    public static readonly PacketEqualityComparer Instance = new();

    public bool Equals(RaftPacket x, RaftPacket y)
    {
        return Check(( dynamic ) x, ( dynamic ) y);
    }

    public bool Check(AppendEntriesRequestPacket first, AppendEntriesRequestPacket second)
    {
        var (f, s) = ( first.Request, second.Request );
        return f.Term == s.Term
            && f.LeaderCommit == s.LeaderCommit
            && f.LeaderId == s.LeaderId
            && f.PrevLogEntryInfo == s.PrevLogEntryInfo
            && f.Entries.SequenceEqual(s.Entries, LogEntryEqualityComparer.Instance);
    }

    public bool Check(AppendEntriesResponsePacket first, AppendEntriesResponsePacket second)
    {
        return first.Response.Success == second.Response.Success && first.Response.Term == second.Response.Term;
    }

    public bool Check(RequestVoteRequestPacket first, RequestVoteRequestPacket second)
    {
        return first.Request.CandidateTerm == second.Request.CandidateTerm
            && first.Request.CandidateId == second.Request.CandidateId
            && first.Request.LastLogEntryInfo == second.Request.LastLogEntryInfo;
    }

    public bool Check(RequestVoteResponsePacket first, RequestVoteResponsePacket second)
    {
        return first.Response.VoteGranted == second.Response.VoteGranted
            && first.Response.CurrentTerm == second.Response.CurrentTerm;
    }

    public bool Check(ConnectRequestPacket first, ConnectRequestPacket second)
    {
        return first.Id == second.Id;
    }

    public bool Check(ConnectResponsePacket first, ConnectResponsePacket second)
    {
        return first.Success == second.Success;
    }

    public bool Check(InstallSnapshotRequestPacket first, InstallSnapshotRequestPacket second)
    {
        return first.LeaderId == second.LeaderId
            && first.Term == second.Term
            && first.LastEntry == second.LastEntry;
    }

    public bool Check(InstallSnapshotChunkPacket first, InstallSnapshotChunkPacket second)
    {
        return first.Chunk
                    .ToArray()
                    .SequenceEqual(second.Chunk.ToArray());
    }

    public bool Check(InstallSnapshotResponsePacket first, InstallSnapshotResponsePacket second)
    {
        return first.CurrentTerm == second.CurrentTerm;
    }

    public int GetHashCode(RaftPacket obj)
    {
        return ( int ) obj.PacketType;
    }
}