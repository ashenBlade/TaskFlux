using Raft.Core.Log;

namespace Raft.Log.InMemory;

public class InMemoryLog: ILog
{
    private List<LogEntry> _log;
    public IReadOnlyList<LogEntry> Entries => _log;

    public void AppendUpdateRange(IEnumerable<LogEntry> entries, int startIndex)
    {
        if (_log.Count < startIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(startIndex),
                "Размер лога меньше начального индекса добавления новых записей");
        }

        // Стартовый индекс может указывать на конец лога, тогда просто добавим новые записи
        if (startIndex == _log.Count)
        {
            _log.AddRange(entries);
        }
        else
        {
            // В противном случае, нужно не только добавить новые записи, 
            // но и обновить сам лог, т.к. добавленные записи, должны быть последними

            // Новый записи полностью входят в старые записи
            // TODO: оптимизировать
            var previous = _log.GetRange(0, startIndex);
            previous.AddRange(entries.ToArray());
            _log = previous;
        }
    }

    public LogEntryInfo Append(LogEntry entry)
    {
        _log.Add(entry);
        return new LogEntryInfo(entry.Term, _log.Count);
    }
    
    public InMemoryLog(IEnumerable<LogEntry> entries)
    {
        _log = new List<LogEntry>(entries);
    }
    
    public bool IsConsistentWith(LogEntryInfo prefix)
    {
        var logConsistent = prefix.Index == -1 || // Лог отправителя был изначально пуст 
                            ( prefix.Index < Entries.Count // Наш лог не меньше (используется PrevLogEntry, поэтому нет +1)
                           && prefix.Term == Entries[prefix.Index].Term ); // Термы записей одинковые

        return logConsistent;
    }

    public LogEntryCheckResult Check(LogEntryInfo entryInfo)
    {
        // Если записей в логе нет
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

    private LogEntryInfo GetLogEntryInfoAtIndex(Index index)
    {
        return new LogEntryInfo(_log[index].Term, index.GetOffset(0));
    }
    
    public LogEntryInfo LastEntry => _log.Count > 0
                                         ? GetLogEntryInfoAtIndex(^1)
                                         : LogEntryInfo.Tomb;
    public int CommitIndex { get; set; }
    public int LastApplied { get; set; } = 0;

  

    public IReadOnlyList<LogEntry> this[int index] =>
        Array.Empty<LogEntry>();

    public IReadOnlyList<LogEntry> GetFrom(int index)
    {
        if (_log.Count <= index)
        {
            return Array.Empty<LogEntry>();
        }

        return _log.GetRange(index, _log.Count - index);
    }

    public void Commit(int index)
    {
        CommitIndex = index;
    }
}