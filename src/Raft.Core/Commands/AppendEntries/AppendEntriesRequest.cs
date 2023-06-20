using Raft.Core.Log;

namespace Raft.Core.Commands.AppendEntries;

public record AppendEntriesRequest(Term Term,
                                   int LeaderCommit,
                                   NodeId LeaderId,
                                   LogEntry PrevLogEntry,
                                   string[] Entries)
{
    public static AppendEntriesRequest Heartbeat(Term term,
                                                 int leaderCommit,
                                                 NodeId leaderId,
                                                 LogEntry prevLogEntry) => new(term, leaderCommit,
        leaderId, prevLogEntry, Array.Empty<string>());
}