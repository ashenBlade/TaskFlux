using Raft.Core;
using Raft.Core.Log;

namespace Raft.Server.Infrastructure;

public class StubLog: ILog
{
    public LogEntry Append(Term term)
    {
        return LogEntry.Empty;
    }

    public LogEntryCheckResult Check(LogEntry entry)
    {
        return LogEntryCheckResult.Contains;
    }

    public LogEntry LastLogEntry => LogEntry.Empty;
    public int CommitIndex
    {
        get => 0;
        set { }
    }

    public int LastApplied => 0;
}