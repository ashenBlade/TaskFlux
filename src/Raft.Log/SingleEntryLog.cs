using Raft.Core;
using Raft.Core.Log;

namespace Raft.Log;

public class SingleEntryLog: ILog
{
    private readonly LogEntry _entry;

    public SingleEntryLog(LogEntry entry)
    {
        _entry = entry;
    }
    
    public bool Contains(LogEntry logEntry)
    {
        return _entry == logEntry;
    }

    public LogEntryCheckResult Check(LogEntry entry)
    {
        if (_entry.Index < entry.Index)
        {
            return LogEntryCheckResult.NotFound;
        }

        if (_entry.Term != entry.Term)
        {
            return LogEntryCheckResult.Conflict;
        }

        return LogEntryCheckResult.Contains;
    }

    public LogEntry LastLogEntry => _entry;
    public int CommitIndex { get; set; } = 0;
    public int LastApplied { get; } = 0;
}