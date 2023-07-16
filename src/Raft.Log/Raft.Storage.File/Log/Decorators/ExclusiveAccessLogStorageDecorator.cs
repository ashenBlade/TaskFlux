using Raft.Core.Log;

namespace Raft.Storage.File.Log.Decorators;

public class ExclusiveAccessLogStorageDecorator: ILogStorage
{
    private readonly ILogStorage _log;
    private SpinLock _lock;

    public ExclusiveAccessLogStorageDecorator(ILogStorage log)
    {
        _log = log;
    }

    public int Count => _log.Count;

    public LogEntryInfo Append(LogEntry entry)
    {
        var taken = false;
        try
        {
            _lock.Enter(ref taken);
            return _log.Append(entry);
        }
        finally
        {
            if (taken)
            {
                _lock.Exit();
            }
        }
    }

    public LogEntryInfo AppendRange(IEnumerable<LogEntry> entries)
    {
        var taken = false;
        try
        {
            _lock.Enter(ref taken);
            return _log.AppendRange(entries);
        }
        finally
        {
            if (taken)
            {
                _lock.Exit();
            }
        }
    }

    public IReadOnlyList<LogEntry> ReadAll()
    {
        var taken = false;
        try
        {
            _lock.Enter(ref taken);
            return _log.ReadAll();
        }
        finally
        {
            if (taken)
            {
                _lock.Exit();
            }
        }
    }

    public LogEntryInfo GetPrecedingLogEntryInfo(int nextIndex)
    {
        var taken = false;
        try
        {
            _lock.Enter(ref taken);
            return _log.GetPrecedingLogEntryInfo(nextIndex);
        }
        finally
        {
            if (taken)
            {
                _lock.Exit();
            }
        }
    }

    public LogEntryInfo GetLastLogEntry()
    {
        var taken = false;
        try
        {
            _lock.Enter(ref taken);
            return _log.GetLastLogEntry();
        }
        finally
        {
            if (taken)
            {
                _lock.Exit();
            }
        }
    }

    public IReadOnlyList<LogEntry> ReadFrom(int startIndex)
    {
        var taken = false;
        try
        {
            _lock.Enter(ref taken);
            return _log.ReadFrom(startIndex);
        }
        finally
        {
            if (taken)
            {
                _lock.Exit();
            }
        }
    }

    public LogEntryInfo GetAt(int index)
    {
        var taken = false;
        try
        {
            _lock.Enter(ref taken);
            return _log.GetAt(index);
        }
        finally
        {
            if (taken)
            {
                _lock.Exit();
            }
        }
    }

    public IReadOnlyList<LogEntry> GetRange(int start, int end)
    {
        var taken = false;
        try
        {
            _lock.Enter(ref taken);
            return _log.GetRange(start, end);
        }
        finally
        {
            if (taken)
            {
                _lock.Exit();
            }
        }
    }

    public void Flush(int index)
    {
        var taken = false;
        try
        {
            _lock.Enter(ref taken);
            _log.Flush(index);
        }
        finally
        {
            if (taken)
            {
                _lock.Exit();
            }
        }
    }
}