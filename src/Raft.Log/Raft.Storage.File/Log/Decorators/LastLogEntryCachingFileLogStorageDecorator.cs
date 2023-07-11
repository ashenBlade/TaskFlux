using Raft.Core.Log;

namespace Raft.Storage.File.Log.Decorators;

/// <summary>
/// Декоратор для быстрого доступа к последней записи в логе.
/// Используется стратегия Read-Through
/// </summary>
public class LastLogEntryCachingFileLogStorageDecorator: ILogStorage
{
    private readonly ILogStorage _storage;
    private LogEntryInfo _lastLogEntry;
    
    public LastLogEntryCachingFileLogStorageDecorator(ILogStorage storage)
    {
        _lastLogEntry = storage.GetLastLogEntry();
        _storage = storage;
    }

    public LogEntryInfo Append(LogEntry entry)
    {
        var last = _storage.Append(entry);
        _lastLogEntry = last;
        return last;
    }

    public LogEntryInfo AppendRange(IEnumerable<LogEntry> entries, int index)
    {
        var last = _storage.AppendRange(entries, index);
        _lastLogEntry = last;
        return last;
    }

    public IReadOnlyList<LogEntry> ReadAll()
    {
        return _storage.ReadAll();
    }

    public LogEntryInfo GetPrecedingLogEntryInfo(int nextIndex)
    {
        if (nextIndex == _lastLogEntry.Index + 1)
        {
            return _lastLogEntry;
        }
        
        return _storage.GetPrecedingLogEntryInfo(nextIndex);
    }

    public LogEntryInfo GetLastLogEntry()
    {
        return _lastLogEntry;
    }

    public IReadOnlyList<LogEntry> ReadFrom(int startIndex)
    {
        return _storage.ReadFrom(startIndex);
    }

    public LogEntryInfo GetAt(int index)
    {
        if (index == _lastLogEntry.Index)
        {
            return _lastLogEntry;
        }
        return _storage.GetAt(index);
    }

    public IReadOnlyList<LogEntry> GetRange(int start, int end)
    {
        return _storage.GetRange(start, end);
    }

    public void Flush(int index)
    {
        _storage.Flush(index);
    }
}