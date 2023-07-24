using System.Runtime.CompilerServices;
using Consensus.Core.Log;
using Consensus.StateMachine;

[assembly: InternalsVisibleTo("Consensus.Log.Tests")]
namespace Consensus.Log;

public class StorageLog: ILog
{
    /// <summary>
    /// Персистентное хранилище записей лога
    /// </summary>
    private readonly ILogStorage _storage;
    public LogEntryInfo LastEntry => _buffer.Count > 0 
                                         ? new LogEntryInfo(_buffer[^1].Term, CommitIndex + _buffer.Count)
                                         : _storage.GetLastLogEntry();

    public int CommitIndex => _storage.Count - 1;
    public int LastApplied { get; internal set; } = LogEntryInfo.TombIndex;
    public IReadOnlyList<LogEntry> ReadLog() => _storage.ReadAll();

    /// <summary>
    /// Временный буфер для незакоммиченных записей
    /// </summary>
    private readonly List<LogEntry> _buffer = new();
    
    public StorageLog(ILogStorage storage )
    {
        _storage = storage;
    }

    // Для тестов
    internal StorageLog(ILogStorage storage, List<LogEntry> buffer)
    {
        _storage = storage;
        _buffer = buffer;
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

    public void InsertRange(IEnumerable<LogEntry> entries, int startIndex)
    {
        var actualIndex = startIndex - _storage.Count;
        var removeCount = _buffer.Count - actualIndex;
        _buffer.RemoveRange(actualIndex, removeCount);
        _buffer.AddRange(entries);
    }

    public LogEntryInfo Append(LogEntry entry)
    {
        var newIndex = _storage.Count + _buffer.Count;
        _buffer.Add(entry);
        return new LogEntryInfo(entry.Term, newIndex);
    }

    public bool Contains(LogEntryInfo prefix)
    {
        if (prefix.IsTomb)
        {
            // Лог отправителя был изначально пуст
            return true;
        }

        
        if (prefix.Index <= LastEntry.Index && // Наш лог не меньше (используется PrevLogEntry, поэтому нет +1)
            prefix.Term == GetLogEntryInfoAtIndex(prefix.Index).Term) // Термы записей одинаковые
        {
            return true;
        }

        return false;
    }

    private LogEntryInfo GetLogEntryInfoAtIndex(int index)
    {
        var storageLastEntry = _storage.GetLastLogEntry();
        if (index <= storageLastEntry.Index)
        {
            return _storage.GetAt(index);
        }

        var bufferEntry = _buffer[index - _storage.Count];
        return new LogEntryInfo(bufferEntry.Term, index);
    }
    
    public IReadOnlyList<LogEntry> GetFrom(int index)
    {
        if (index <= _storage.Count)
        {
            // Читаем часть из диска
            var logEntries = _storage.ReadFrom(index);
            var result = new List<LogEntry>(logEntries.Count + _buffer.Count);
            result.AddRange(logEntries);
            // Прибаляем весь лог в памяти
            result.AddRange(_buffer);
            // Конкатенируем
            return result;
        }
        
        // Требуемые записи только в памяти 
        var bufferStartIndex = index - _storage.Count;
        if (index <= _buffer.Count)
        {
            // Берем часть из лога
            var logPart = _buffer.GetRange(bufferStartIndex, _buffer.Count - bufferStartIndex);
            
            // Возвращаем
            return logPart;
        }
        else
        {
            // Индекс неверный
            throw new ArgumentOutOfRangeException(nameof(index), index,
                "Указанный индекс больше чем последний индекс лога");
        }
    }

    public void Commit(int index)
    {
        var removeCount = index - _storage.Count + 1;
        if (removeCount == 0)
        {
            return;
        }

        if (_buffer.Count < removeCount)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index,
                "Указанный индекс больше количества записей в логе");
        }
        
        var notCommitted = _buffer.GetRange(0, removeCount);
        _storage.AppendRange(notCommitted);
        _buffer.RemoveRange(0, removeCount);
    }

    public LogEntryInfo GetPrecedingEntryInfo(int nextIndex)
    {
        if (nextIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex, "Следующий индекс лога не может быть отрицательным");
        }

        if (_storage.Count + _buffer.Count + 1 < nextIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс лога не может быть больше числа записей в логе + 1");
        }
        
        if (nextIndex == 0)
        {
            return LogEntryInfo.Tomb;
        }
        
        var index = nextIndex - 1;

        // Запись находится в логе
        if (_storage.Count <= index)
        {
            var bufferIndex = index - _storage.Count;
            var bufferEntry = _buffer[bufferIndex];
            return new LogEntryInfo(bufferEntry.Term, index);
        }

        return _storage.GetAt(index);
    }

    public IReadOnlyList<LogEntry> GetNotApplied()
    {
        if (CommitIndex <= LastApplied || CommitIndex == LogEntryInfo.TombIndex)
        {
            return Array.Empty<LogEntry>();
        }

        return _storage.ReadFrom(LastApplied + 1);
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