using Raft.Core;
using Raft.Core.Log;

namespace Raft.Log.InMemory;

public class InMemoryLog: ILog
{
    private readonly List<LogEntry> _log = new() {LogEntry.Empty};

    public InMemoryLog()
    { }

    public LogEntry Append(Term term)
    {
        return LogEntry.Empty;
    }
    
    public InMemoryLog(IEnumerable<LogEntry> entries)
    {
        
        _log = new List<LogEntry>() {LogEntry.Empty};
        _log.AddRange(entries);
    }

    public InMemoryLog(IEnumerable<Term> terms)
    {
        _log = new List<LogEntry>() {LogEntry.Empty};
        _log.AddRange(terms.Select((t, i) => new LogEntry(t, i + 1)));
    }

    public void Add(Term term)
    {
        _log.Add(new LogEntry(term, _log.Count));
    }
    
    public LogEntryCheckResult Check(LogEntry entry)
    {
        if (_log.Count < entry.Index)
        {
            return LogEntryCheckResult.NotFound;
        }

        var index = entry.Index - 1;
        var existing = _log[index];
        
        // Если по одному и тому же индексу лежат данные с одним и тем же термом - 
        // данные, хранящиеся в записях, - одинаковые
        return existing.Term != entry.Term
                   ? LogEntryCheckResult.Conflict
                   : LogEntryCheckResult.Contains;
    }

    public LogEntry LastLogEntry => _log[^1];
    public int CommitIndex { get; set; } = 0;
    public int LastApplied { get; } = 0;
}