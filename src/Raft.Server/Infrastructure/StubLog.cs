using Raft.Core;
using Raft.Core.Log;

namespace Raft.Server.Infrastructure;

public class StubLog: ILog
{
    public LogEntryInfo Append(Term term, string command)
    {
        return LogEntryInfo.Empty;
    }

    public LogEntryCheckResult Check(LogEntryInfo entryInfo)
    {
        return LogEntryCheckResult.Contains;
    }

    public LogEntryInfo LastLogEntryInfo => LogEntryInfo.Empty;
    public int CommitIndex
    {
        get => 0;
        set { }
    }

    public int LastApplied => 0;

    public Memory<LogEntry> this[Range range] =>
        Memory<LogEntry>.Empty;

    public Memory<LogEntry> this[int index] =>
        Memory<LogEntry>.Empty;
}