namespace Raft.Core.Log;

public class StorageLog: ILog
{
    private readonly ILogStorage _storage;
    public LogEntryInfo LastEntry => _storage.GetLastLogEntry();
    public int CommitIndex { get; private set; } = LogEntryInfo.TombIndex;
    public int LastApplied { get; private set; } = LogEntryInfo.TombIndex;
    public IReadOnlyList<LogEntry> ReadLog() => _storage.ReadAll();

    public StorageLog(ILogStorage storage)
    {
        _storage = storage;
    }

    public bool Conflicts(LogEntryInfo prefix)
    {
        // Неважно на каком индексе последний элемент.
        // Если он последний, то наши старые могут быть заменены
        
        // Наш:      | 1 | 1 | 2 | 3 |
        // Другой 1: | 1 | 1 | 2 | 3 | 4 | 5 |
        // Другой 2: | 1 | 5 | 
        if (LastEntry.Term < prefix.Term)
        {
            return false;
        }

        // В противном случае голос отдает только за тех,
        // префикс лога, которых не меньше нашего
        
        // Наш:      | 1 | 1 | 2 | 3 |
        // Другой 1: | 1 | 1 | 2 | 3 | 3 | 3 |
        // Другой 2: | 1 | 1 | 2 | 3 |
        if (prefix.Term == LastEntry.Term && 
            LastEntry.Index <= prefix.Index)
        {
            return false;
        }
        
        return true;
    }

    public void AppendUpdateRange(IEnumerable<LogEntry> entries, int startIndex)
    {
        _storage.AppendRange(entries, startIndex);
    }

    public LogEntryInfo Append(LogEntry entry)
    {
        return _storage.Append(entry);
    }
    

    public bool Contains(LogEntryInfo prefix)
    {
        if (prefix.IsTomb)
        {
            // Лог отправителя был изначально пуст
            return true;
        }

        
        if (prefix.Index <= LastEntry.Index && // Наш лог не меньше (используется PrevLogEntry, поэтому нет +1)
            prefix.Term == _storage.GetAt(prefix.Index).Term) // Термы записей одинаковые
        {
            return true;
        }

        return false;
    }
    
    public IReadOnlyList<LogEntry> GetFrom(int index)
    {
        if (LastEntry.Index < index)
        {
            return Array.Empty<LogEntry>();
        }
        return _storage.ReadFrom(index);
    }


    public CommitDelta Commit(int index)
    {
        if (LastEntry.Index < index)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index,
                $"Невозможно выполнить коммит: переданный индекс больше количества хранимых записей - {LastEntry.Index}");
        }

        var previous = CommitIndex;
        if (CommitIndex < index)
        {
            CommitIndex = index;
        }

        return new CommitDelta(previous, index);
    }

    public LogEntryInfo GetPrecedingEntryInfo(int nextIndex)
    {
        if (nextIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex, "Следующий индекс лога не может быть отрицательным");
        }
        
        return _storage.GetPrecedingLogEntryInfo(nextIndex);
    }

    public void SetLastApplied(int index)
    {
        if (index < LogEntryInfo.TombIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index, "Переданный индекс меньше TombIndex");
        }
        LastApplied = index;
    }
}