using TaskFlux.Core;

namespace TaskFlux.Consensus.Commands.AppendEntries;

public record AppendEntriesRequest(
    Term Term,
    Lsn LeaderCommit,
    NodeId LeaderId,
    LogEntryInfo PrevLogEntryInfo,
    IReadOnlyList<LogEntry> Entries)
{
    public static AppendEntriesRequest Heartbeat(Term term,
        Lsn leaderCommit,
        NodeId leaderId,
        LogEntryInfo prevLogEntryInfo)
        => new(term, leaderCommit, leaderId, prevLogEntryInfo, Array.Empty<LogEntry>());
}