using System.Runtime.CompilerServices;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.LogFileCheckStrategy;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using TaskFlux.Core;

[assembly: InternalsVisibleTo("Consensus.Storage.Tests")]

namespace Consensus.Raft.Persistence;

public class StoragePersistenceFacade : IPersistenceFacade
{
    /// <summary>
    /// Файл лога команд - `consensus/raft.log`
    /// </summary>
    private readonly FileLogStorage _logStorage;

    /// <summary>
    /// Файл метаданных - `consensus/raft.metadata`
    /// </summary>
    private readonly IMetadataStorage _metadataStorage;

    /// <summary>
    /// Файл снапшота - `consensus/raft.snapshot`
    /// </summary>
    private readonly FileSystemSnapshotStorage _snapshotStorage;

    /// <summary>
    /// Временный буфер для незакоммиченных записей
    /// </summary>
    private readonly List<LogEntry> _buffer = new();

    /// <summary>
    /// Метод для проверки превышения размера файла лога.
    /// Подобная логика нужна для тестов
    /// </summary>
    private readonly ILogFileSizeChecker _sizeChecker;

    public LogEntryInfo LastEntry => _buffer.Count > 0
                                         ? new LogEntryInfo(_buffer[^1].Term, CommitIndex + _buffer.Count)
                                         : _logStorage.GetLastLogEntry();

    public int CommitIndex => _logStorage.Count - 1;
    public int LastAppliedIndex { get; private set; } = LogEntryInfo.TombIndex;
    public ulong LogFileSize => _logStorage.FileSize;

    public LogEntryInfo LastApplied => LastAppliedIndex == LogEntryInfo.TombIndex
                                           ? LogEntryInfo.Tomb
                                           : GetLogEntryInfoAtIndex(LastAppliedIndex);

    public Term CurrentTerm => _metadataStorage.Term;
    public NodeId? VotedFor => _metadataStorage.VotedFor;
    internal FileSystemSnapshotStorage SnapshotStorage => _snapshotStorage;
    internal FileLogStorage LogStorage => _logStorage;

    public StoragePersistenceFacade(FileLogStorage logStorage,
                                    IMetadataStorage metadataStorage,
                                    FileSystemSnapshotStorage snapshotStorage)
    {
        _logStorage = logStorage;
        _metadataStorage = metadataStorage;
        _snapshotStorage = snapshotStorage;
        _sizeChecker = SizeLogFileSizeChecker.MaxLogFileSize;
    }

    internal StoragePersistenceFacade(FileLogStorage logStorage,
                                      IMetadataStorage metadataStorage,
                                      FileSystemSnapshotStorage snapshotStorage,
                                      ILogFileSizeChecker sizeChecker)
    {
        _logStorage = logStorage;
        _metadataStorage = metadataStorage;
        _snapshotStorage = snapshotStorage;
        _sizeChecker = sizeChecker;
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
        if (prefix.Term == LastEntry.Term && LastEntry.Index <= prefix.Index)
        {
            return false;
        }

        return true;
    }

    public void InsertRange(IEnumerable<LogEntry> entries, int startIndex)
    {
        // Индекс, с которого начинаются записи в буфере
        var bufferStartIndex = startIndex - _logStorage.Count;

        if (bufferStartIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(startIndex), startIndex,
                "Нельзя перезаписать закоммиченные записи");
        }

        // Сколько элементов в буфере нужно удалить
        var removeCount = _buffer.Count - bufferStartIndex;

        // Удаляем срез
        _buffer.RemoveRange(bufferStartIndex, removeCount);

        // Вставляем в это место новые записи
        _buffer.AddRange(entries);
    }

    public LogEntryInfo AppendBuffer(LogEntry entry)
    {
        var newIndex = _logStorage.Count + _buffer.Count;
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


        if (prefix.Index <= LastEntry.Index
         && // Наш лог не меньше (используется PrevLogEntry, поэтому нет +1)
            prefix.Term == GetLogEntryInfoAtIndex(prefix.Index).Term) // Термы записей одинаковые
        {
            return true;
        }

        return false;
    }

    private LogEntryInfo GetLogEntryInfoAtIndex(int index)
    {
        var storageLastEntry = _logStorage.GetLastLogEntry();
        if (index <= storageLastEntry.Index)
        {
            return _logStorage.GetInfoAt(index);
        }

        var bufferEntry = _buffer[index - _logStorage.Count];
        return new LogEntryInfo(bufferEntry.Term, index);
    }

    [Obsolete("Переходи на Try версию", true)]
    public IReadOnlyList<LogEntry> GetFrom(int index)
    {
        if (index < _logStorage.Count)
        {
            // Читаем часть из диска
            var logEntries = _logStorage.ReadFrom(index);
            var result = new List<LogEntry>(logEntries.Count + _buffer.Count);
            result.AddRange(logEntries);
            // Прибаляем весь лог в памяти
            result.AddRange(_buffer);
            // Конкатенируем
            return result;
        }

        // Требуемые записи только в буфере (незакоммичены)
        var bufferStartIndex = index - _logStorage.Count;
        if (bufferStartIndex <= _buffer.Count)
        {
            // Берем часть из лога
            var logPart = _buffer.GetRange(bufferStartIndex, _buffer.Count - bufferStartIndex);

            // Возвращаем
            return logPart;
        }

        // Индекс неверный
        throw new ArgumentOutOfRangeException(nameof(index), index,
            "Указанный индекс больше чем последний индекс лога");
    }

    public bool TryGetFrom(int globalIndex, out IReadOnlyList<LogEntry> entries)
    {
        var localIndex = CalculateLocalIndex();

        if (localIndex < 0)
        {
            entries = Array.Empty<LogEntry>();
            return false;
        }

        if (localIndex < _logStorage.Count)
        {
            // Читаем часть из диска
            var logEntries = _logStorage.ReadFrom(localIndex);
            var result = new List<LogEntry>(logEntries.Count + _buffer.Count);
            result.AddRange(logEntries);
            // Прибаляем весь лог в памяти
            result.AddRange(_buffer);
            // Конкатенируем
            entries = result;
            return true;
        }

        // Требуемые записи только в буфере (незакоммичены)
        var bufferStartIndex = localIndex - _logStorage.Count;
        if (bufferStartIndex <= _buffer.Count)
        {
            // Берем часть из лога
            var logPart = _buffer.GetRange(bufferStartIndex, _buffer.Count - bufferStartIndex);

            // Возвращаем
            entries = logPart;
            return true;
        }

        // Индекс неверный
        entries = Array.Empty<LogEntry>();
        return false;

        int CalculateLocalIndex()
        {
            /*
             * Я разделяю индекс на 2 типа: локальный и глобальный.
             * - Глобальный индекс - индекс среди ВСЕХ записей
             * - Локальный индекс - индекс для поиска записей среди лога и буфера
             *
             * Последний индекс в снапшоте является стартовым индексом для локального индекса.
             * Чтобы получить локальный индекс нужно из глобального индекса вычесть последний индекс снапшота и 1:
             * localIndex = globalIndex - Snapshot.LastIncludedIndex - 1
             * Последняя единица вычитается, т.к. индексация начинается с 0:
             *
             * Примеры:
             * Глобальный индекс: 10, Индекс снапшота: 3, Локальный индекс: 6
             * Глобальный - | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 |
             *              |Данные снапшота|
             * Локальный  -                 | 0 | 1 | 2 | 3 | 4 | 5 | 6  | 7  |
             *
             * Снапшота еще нет, то локальный и глобальный индексы совпадают
             */
            if (SnapshotStorage.LastLogEntry.IsTomb)
            {
                return globalIndex;
            }

            return globalIndex - SnapshotStorage.LastLogEntry.Index - 1;
        }
    }

    public void Commit(int index)
    {
        var removeCount = index - _logStorage.Count + 1;
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
        _logStorage.AppendRange(notCommitted);
        _buffer.RemoveRange(0, removeCount);
    }

    public LogEntryInfo GetPrecedingEntryInfo(int nextIndex)
    {
        if (nextIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс лога не может быть отрицательным");
        }

        if (_logStorage.Count + _buffer.Count + 1 < nextIndex)
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
        if (_logStorage.Count <= index)
        {
            var bufferIndex = index - _logStorage.Count;
            var bufferEntry = _buffer[bufferIndex];
            return new LogEntryInfo(bufferEntry.Term, index);
        }

        return _logStorage.GetInfoAt(index);
    }

    public IReadOnlyList<LogEntry> GetNotApplied()
    {
        if (CommitIndex <= LastAppliedIndex || CommitIndex == LogEntryInfo.TombIndex)
        {
            return Array.Empty<LogEntry>();
        }

        return _logStorage.ReadFrom(LastAppliedIndex + 1);
    }

    public void SetLastApplied(int index)
    {
        if (index < LogEntryInfo.TombIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index, "Переданный индекс меньше TombIndex");
        }

        LastAppliedIndex = index;
    }

    public void SaveSnapshot(LogEntryInfo lastLogEntry, ISnapshot snapshot, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();

        // 1. Создать временный файл
        var tempFile = _snapshotStorage.CreateTempSnapshotFile();

        // 2. Записать заголовок: маркер, индекс, терм
        tempFile.Initialize(lastLogEntry);

        // 3. Записываем сами данные на диск
        try
        {
            tempFile.WriteSnapshot(snapshot, token);
        }
        catch (OperationCanceledException)
        {
            // 3.1. Если роль изменилась/появился новый лидер и т.д. - прекрать создание нового снапшота
            tempFile.Discard();
            return;
        }

        // 4. Переименовать файл в нужное имя
        tempFile.Save();
    }

    public void ClearCommandLog()
    {
        // Очищаем непримененные команды
        _buffer.Clear();
        // Очищаем сам файл лога
        _logStorage.ClearCommandLog();
        LastAppliedIndex = 0;
    }

    public bool TryGetSnapshot(out ISnapshot snapshot)
    {
        if (!_snapshotStorage.LastLogEntry.IsTomb)
        {
            snapshot = _snapshotStorage.GetSnapshot();
            return true;
        }

        snapshot = default!;
        return false;
    }

    public void UpdateState(Term newTerm, NodeId? votedFor)
    {
        _metadataStorage.Update(newTerm, votedFor);
    }

    public bool IsLogFileSizeExceeded()
    {
        return _sizeChecker.IsLogFileSizeExceeded(_logStorage.FileSize);
    }

    // Для тестов
    internal (int LastIndex, Term LastTerm, byte[] SnapshotData) ReadSnapshotFileTest()
    {
        return _snapshotStorage.ReadAllData();
    }

    /// <summary>
    /// Прочитать все записи лога команд: из файла и памяти.
    /// Используется для тестов
    /// </summary>
    /// <returns>Записи лога</returns>
    internal IReadOnlyList<LogEntry> ReadLogFull() => _logStorage.ReadAll()       // Файл
                                                                 .Concat(_buffer) // Не закоммиченные в памяти
                                                                 .ToList();

    /// <summary>
    /// Прочитать все записи в буфере записей команд.
    /// Используется только для тестов.
    /// Введено на всякий случай, если для внутреннего массива будут использоваться какие-нибудь `Null` значения
    /// </summary>
    /// <returns>Записи внутреннего буфера команд</returns>
    internal List<LogEntry> ReadLogBufferTest()
    {
        return _buffer;
    }

    /// <summary>
    /// Выставить буфер в памяти переданным списком элементов.
    /// Вызывать метод нужно перед всеми операциями.
    /// </summary>
    /// <param name="buffer">Новый буфер в памяти</param>
    /// <remarks>Используется для тестов</remarks>
    /// <exception cref="InvalidOperationException">Буфер не был пустым</exception>
    internal void SetupBufferTest(IEnumerable<LogEntry> buffer)
    {
        if (_buffer.Count > 0)
        {
            // Эта штука нужна в первую очередь, чтобы случайно не затереть данные,
            // если будет использоваться какой-нибудь Null паттерн
            throw new InvalidOperationException("Буфер не пустой");
        }

        _buffer.AddRange(buffer);
    }

    /// <summary>
    /// Прочитать записи из файла лога.
    /// Используется для тестов
    /// </summary>
    internal IEnumerable<LogEntry> ReadLogFileTest()
    {
        return _logStorage.ReadAllTest();
    }
}