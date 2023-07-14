using Raft.Core.Log;
using Raft.Log;

namespace Raft.Buffer.InMemory;

// public class InMemoryLogBuffer: ILogBuffer
// {
//     public const int LogEmptyIndex = -1;
//     private int _startIndex;
//
//     public InMemoryLogBuffer(int startIndex)
//     {
//         _startIndex = startIndex;
//     }
//     
//     private readonly List<LogEntry> _list = new();
//
//     public LogEntryInfo LastEntryInfo => _list.Count > 0 
//                                              ? new LogEntryInfo(_list[], last.Value.Index)
//                                              : throw new InvalidOperationException("В буфере лога нет записей");
//
//     private LogEntryInfo GetLastEntryInfoCore()
//     {
//         if (_list.Count == 0)
//         {
//             throw new InvalidOperationException("В буфере лога нет записей");
//         }
//
//         var last = _list[^1];
//         return new LogEntryInfo(last.Term, _startIndex + _list.Count - 1);
//     }
//     
//     public LogEntryInfo GetAt(int index)
//     {
//         var innerIndex = index - _startIndex;
//         if (innerIndex < 0)
//         {
//             throw new ArgumentOutOfRangeException(nameof(index), index,
//                 "Запись с указанным индексом не находится в памяти");
//         }
//
//         if (_list.Count < innerIndex)
//         {
//             throw new ArgumentOutOfRangeException(nameof(index), index,
//                 "Указанный индекс больше максимального хранимого индекса в памяти");
//         }
//         
//         var inner = _list.
//         return new LogEntryInfo()
//     }
//
//     private LogEntryIndex GetAtIndexCore
//     
//     public bool TryGetPrecedingLogEntryInfo(int nextIndex, out LogEntryInfo entry)
//     {
//         if (_startIndex == LogEmptyIndex || 
//             nextIndex == _startIndex)
//         {
//             entry = LogEntryInfo.Tomb;
//             return false;
//         }
//
//         var originalIndex = nextIndex;
//         nextIndex -= _startIndex;
//         
//         if (_list.Count + 1 < nextIndex)
//         {
//             throw new ArgumentOutOfRangeException(nameof(nextIndex), originalIndex,
//                 "Индекс следующей записи больше размера лога");
//         }
//         
//         var logEntry = _list[nextIndex - 1];
//         entry = new LogEntryInfo(logEntry.Term, _startIndex + nextIndex);
//         return true;
//     }
//
//     public IReadOnlyList<LogEntry> ReadFrom(int index)
//     {
//         var currentIndex = index;
//         if (index < _startIndex)
//         {
//             return Array.Empty<LogEntry>();
//         }
//
//         if (_startIndex + _list.Count < index)
//         {
//             return Array.Empty<LogEntry>();
//         }
//
//         var startIndex = index - _startIndex;
//         var count = _list.Count - startIndex;
//         var list = new List<LogEntry>(count);
//         var i = 0;
//         foreach (var entry in _list)
//         {
//             list[i] = entry;
//             i++;
//         }
//
//         return list;
//     }
//
//     public List<LogEntry> TrimUntil(int index)
//     {
//         throw new NotImplementedException();
//     }
//
//     public List<LogEntry> TrimAll()
//     {
//         throw new NotImplementedException();
//     }
//
//     public LogEntryInfo Append(LogEntry entry)
//     {
//         _list.Add(entry);
//         return new LogEntryInfo(entry.Term, _startIndex + _list.Count);
//     }
//
//     public void InsertRange(IEnumerable<LogEntry> entries, int start)
//     {
//         throw new NotImplementedException();
//     }
//
//     public bool TryPeekLast(out LogEntryIndex info)
//     {
//         throw new NotImplementedException();
//     }
// }