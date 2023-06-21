using Raft.Core;
using Raft.Core.Log;

namespace Raft.Log.InMemory;

public class InMemoryLog: ILog
{
    private readonly List<LogEntryInfo> _log = new() {LogEntryInfo.Empty};

    public InMemoryLog()
    { }

    public LogEntryInfo Append(Term term, string message)
    {
        return LogEntryInfo.Empty;
    }
    
    public InMemoryLog(IEnumerable<LogEntryInfo> entries)
    {
        
        _log = new List<LogEntryInfo>() {LogEntryInfo.Empty};
        _log.AddRange(entries);
    }

    public InMemoryLog(IEnumerable<Term> terms)
    {
        _log = new List<LogEntryInfo>() {LogEntryInfo.Empty};
        _log.AddRange(terms.Select((t, i) => new LogEntryInfo(t, i + 1)));
    }

    public void Add(Term term)
    {
        _log.Add(new LogEntryInfo(term, _log.Count));
    }
    
    public LogEntryCheckResult Check(LogEntryInfo entryInfo)
    {
        if (_log.Count < entryInfo.Index)
        {
            return LogEntryCheckResult.NotFound;
        }

        var index = entryInfo.Index - 1;
        var existing = _log[index];
        
        // Если по одному и тому же индексу лежат данные с одним и тем же термом - 
        // данные, хранящиеся в записях, - одинаковые
        return existing.Term != entryInfo.Term
                   ? LogEntryCheckResult.Conflict
                   : LogEntryCheckResult.Contains;
    }

    public LogEntryInfo LastLogEntryInfo => _log[^1];
    public int CommitIndex { get; set; } = 0;
    public int LastApplied { get; } = 0;

    public IReadOnlyList<LogEntry> this[Range range] =>
        Array.Empty<LogEntry>();

    public IReadOnlyList<LogEntry> this[int index] =>
        Array.Empty<LogEntry>();
}