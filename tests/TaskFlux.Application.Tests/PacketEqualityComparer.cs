using TaskFlux.Application.Cluster.Network;
using TaskFlux.Application.Cluster.Network.Packets;

#pragma warning disable CS8767 // Nullability of reference types in type of parameter doesn't match implicitly implemented member (possibly because of nullability attributes).

namespace TaskFlux.Application.Tests;

public class PacketEqualityComparer : IEqualityComparer<NodePacket>
{
    public static readonly PacketEqualityComparer Instance = new();

    public bool Equals(NodePacket x, NodePacket y)
    {
        return Check(( dynamic ) x, ( dynamic ) y);
    }

    private bool Check(AppendEntriesRequestPacket first, AppendEntriesRequestPacket second)
    {
        var (f, s) = ( first.Request, second.Request );
        return f.Term == s.Term
            && f.LeaderCommit == s.LeaderCommit
            && f.LeaderId == s.LeaderId
            && f.PrevLogEntryInfo == s.PrevLogEntryInfo
            && f.Entries.SequenceEqual(s.Entries, LogEntryEqualityComparer.Instance);
    }

    private static bool Check(AppendEntriesResponsePacket first, AppendEntriesResponsePacket second)
    {
        return first.Response.Success == second.Response.Success && first.Response.Term == second.Response.Term;
    }

    private static bool Check(RequestVoteRequestPacket first, RequestVoteRequestPacket second)
    {
        return first.Request.CandidateTerm == second.Request.CandidateTerm
            && first.Request.CandidateId == second.Request.CandidateId
            && first.Request.LastLogEntryInfo == second.Request.LastLogEntryInfo;
    }

    private static bool Check(RequestVoteResponsePacket first, RequestVoteResponsePacket second)
    {
        return first.Response.VoteGranted == second.Response.VoteGranted
            && first.Response.CurrentTerm == second.Response.CurrentTerm;
    }

    private static bool Check(ConnectRequestPacket first, ConnectRequestPacket second)
    {
        return first.Id == second.Id;
    }

    private static bool Check(ConnectResponsePacket first, ConnectResponsePacket second)
    {
        return first.Success == second.Success;
    }

    private static bool Check(InstallSnapshotRequestPacket first, InstallSnapshotRequestPacket second)
    {
        return first.LeaderId == second.LeaderId
            && first.Term == second.Term
            && first.LastEntry == second.LastEntry;
    }

    private static bool Check(InstallSnapshotChunkRequestPacket first, InstallSnapshotChunkRequestPacket second)
    {
        return first.Chunk
                    .ToArray()
                    .SequenceEqual(second.Chunk.ToArray());
    }

    private static bool Check(InstallSnapshotResponsePacket first, InstallSnapshotResponsePacket second)
    {
        return first.CurrentTerm == second.CurrentTerm;
    }

    private static bool Check(RetransmitRequestPacket first, RetransmitRequestPacket second)
        => first.PacketType == second.PacketType;

    public int GetHashCode(NodePacket obj)
    {
        return ( int ) obj.PacketType;
    }
}