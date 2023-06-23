using Raft.Core;
using Raft.Core.Log;

namespace Raft.Server.Infrastructure;

public class StubLog: ILog
{
    public LogEntryInfo Append(Term term, string command)
    {
        return LogEntryInfo.Tomb;
    }

    public IReadOnlyList<LogEntry> Entries { get; }

    public void AppendUpdateRange(IEnumerable<LogEntry> entries, int startIndex)
    {
        
    }
    
    public bool IsConsistentWith(LogEntryInfo prefix)
    {
        return true;
    }

    public LogEntryInfo Append(LogEntry entry)
    {
        return LogEntryInfo.Tomb;
    }

    public LogEntryInfo LastEntry => LogEntryInfo.Tomb;
    public LogEntryInfo PrevLogEntry => LogEntryInfo.Tomb;

    public int CommitIndex
    {
        get => 0;
        set { }
    }

    public int LastApplied
    {
        get => 0;
        set {}
    }

    public IReadOnlyList<LogEntry> this[Range range] =>
        Array.Empty<LogEntry>();

    public IReadOnlyList<LogEntry> this[int index] =>
        Array.Empty<LogEntry>();

    public IReadOnlyList<LogEntry> GetFrom(int index)
    {
        return Array.Empty<LogEntry>();
    }

    public void Commit(int index)
    { }

    public LogEntryInfo GetPrecedingEntryInfo(int nextIndex)
    {
        throw new NotImplementedException();
    }
}