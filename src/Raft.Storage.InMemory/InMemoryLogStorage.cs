using System.Runtime.CompilerServices;
using Raft.Core.Log;

namespace Raft.Storage.InMemory;

public class InMemoryLogStorage: ILogStorage
{
    private List<LogEntry> _log;

    public InMemoryLogStorage(IEnumerable<LogEntry> log)
    {
        _log = log.ToList();
    }

    public InMemoryLogStorage() : this(Enumerable.Empty<LogEntry>()) 
    { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private LogEntryInfo GetLastLogEntryCore()
    {
        if (_log.Count == 0)
        {
            return LogEntryInfo.Tomb;
        }
        var entry = _log[^1];
        return new LogEntryInfo(entry.Term, _log.Count - 1);
    }
    
    public LogEntryInfo Append(LogEntry entry)
    {
        var info = new LogEntryInfo(entry.Term, _log.Count);
        _log.Add(entry);
        return info;
    }

    public LogEntryInfo AppendRange(IEnumerable<LogEntry> entries, int index)
    {
        if (index == _log.Count)
        {
            _log.AddRange(entries);
        }
        else
        {
            _log = _log.Take(index)
                       .Concat(entries)
                       .ToList();
        }

        return GetLastLogEntryCore();
    }

    public IReadOnlyList<LogEntry> ReadAll()
    {
        return _log;
    }

    public LogEntryInfo GetPrecedingLogEntryInfo(int nextIndex)
    {
        return nextIndex == 0 
                   ? LogEntryInfo.Tomb
                   : new LogEntryInfo(_log[nextIndex - 1].Term, nextIndex - 1);
    }

    public LogEntryInfo GetLastLogEntry()
    {
        if (_log.Count == 0)
        {
            return LogEntryInfo.Tomb;
        }

        var last = _log[^1];
        return new LogEntryInfo(last.Term, _log.Count - 1);
    }

    public IReadOnlyList<LogEntry> ReadFrom(int startIndex)
    {
        return _log.GetRange(startIndex, _log.Count - startIndex);
    }

    public LogEntryInfo GetAt(int index)
    {
        return new LogEntryInfo(_log[index].Term, index);
    }

    public IReadOnlyList<LogEntry> GetRange(int start, int end)
    {
        return _log.GetRange(start, end);
    }
}