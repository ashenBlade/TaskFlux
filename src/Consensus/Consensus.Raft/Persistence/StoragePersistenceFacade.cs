using System.Diagnostics;
using System.Runtime.CompilerServices;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.LogFileCheckStrategy;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using TaskFlux.Core;

[assembly: InternalsVisibleTo("Consensus.Storage.Tests")]

namespace Consensus.Raft.Persistence;

public class StoragePersistenceFacade
{
    /// <summary>
    /// Объект для синхронизации работы.
    /// Необходима для синхронизации работы между обработчиками узлов (когда лидер),
    /// или когда параллельно будут приходить запросы от других узлов
    /// </summary>
    private readonly object _lock = new();

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

    /// <summary>
    /// Последняя запись в логе
    /// </summary>
    public LogEntryInfo LastEntry
    {
        get
        {
            lock (_lock)
            {
                if (SnapshotStorage.HasSnapshot)
                {
                    if (_buffer is [_, ..])
                    {
                        var index = SnapshotStorage.LastLogEntry.Index
                                  + _buffer.Count
                                  + _logStorage.Count;
                        return new LogEntryInfo(_buffer[^1].Term, index);
                    }

                    if (0 < _logStorage.Count)
                    {
                        var index = SnapshotStorage.LastLogEntry.Index
                                  + _logStorage.Count;
                        return new LogEntryInfo(_logStorage.GetLastLogEntry().Term, index);
                    }

                    return SnapshotStorage.LastLogEntry;
                }

                // Снапшота нет
                return _buffer is [_, ..]
                           ? new LogEntryInfo(_buffer[^1].Term, _logStorage.Count + _buffer.Count - 1)
                           : _logStorage.GetLastLogEntry();
            }
        }
    }

    /// <summary>
    /// Индекс последней закомиченной записи
    /// </summary>
    public int CommitIndex
    {
        get
        {
            lock (_lock)
            {
                if (SnapshotStorage.HasSnapshot)
                {
                    return SnapshotStorage.LastLogEntry.Index + _logStorage.Count;
                }

                return _logStorage.Count - 1;
            }
        }
    }

    /// <summary>
    /// Индекс последней применной записи журнала.
    /// Обновляется после успешного коммита 
    /// </summary>
    private int LastAppliedGlobalIndex { get; set; }

    /// <summary>
    /// Последняя примененная запись из лога
    /// </summary>
    /// <remarks>
    /// Указанный индекс - глобальный
    /// </remarks>
    private LogEntryInfo LastApplied
    {
        get
        {
            lock (_lock)
            {
                return LastAppliedGlobalIndex == LogEntryInfo.TombIndex
                           ? LogEntryInfo.Tomb
                           : GetLogEntryInfoAtIndex(LastAppliedGlobalIndex);
            }
        }
    }

    /// <summary>
    /// Терм, сохраненный в файле метаданных
    /// </summary>
    public Term CurrentTerm
    {
        get
        {
            lock (_lock)
            {
                return _metadataStorage.Term;
            }
        }
    }

    /// <summary>
    /// Отданный голос, сохраненный в файле метаданных
    /// </summary>
    public NodeId? VotedFor
    {
        get
        {
            lock (_lock)
            {
                return _metadataStorage.VotedFor;
            }
        }
    }

    internal FileSystemSnapshotStorage SnapshotStorage => _snapshotStorage;
    internal FileLogStorage LogStorage => _logStorage;

    public StoragePersistenceFacade(FileLogStorage logStorage,
                                    IMetadataStorage metadataStorage,
                                    FileSystemSnapshotStorage snapshotStorage,
                                    ulong maxLogFileSize = Constants.MaxLogFileSize)
        : this(logStorage, metadataStorage, snapshotStorage, new SizeLogFileSizeChecker(maxLogFileSize))
    {
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
        LastAppliedGlobalIndex = snapshotStorage.HasSnapshot
                                     ? snapshotStorage.LastLogEntry.Index
                                     : LogEntryInfo.TombIndex;
    }

    /// <summary>
    /// Конфликтует ли текущий лог с переданным (используется префикс)
    /// </summary>
    /// <param name="prefix">Последний элемент сравниваемого лога</param>
    /// <returns><c>true</c> - конфликтует, <c>false</c> - иначе</returns>
    /// <remarks>Вызывается в RequestVote для проверки актуальности лога</remarks>
    public bool Conflicts(LogEntryInfo prefix)
    {
        // Неважно на каком индексе последний элемент.
        // Если он последний, то наши старые могут быть заменены

        // Наш:      | 1 | 1 | 2 | 3 |
        // Другой 1: | 1 | 1 | 2 | 3 | 4 | 5 |
        // Другой 2: | 1 | 5 | 
        if (LastEntry.Term < prefix.Term)
        {
            return true;
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

    /// <summary>
    /// Добавить в лог переданные записи, начиная с <see cref="startIndex"/> индекса.
    /// Возможно перезатирание 
    /// </summary>
    /// <param name="entries">Записи, которые необходимо добавить</param>
    /// <param name="startIndex">Индекс, начиная с которого необходимо добавить записи</param>
    public void InsertBufferRange(IEnumerable<LogEntry> entries, int startIndex)
    {
        if (startIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(startIndex), startIndex,
                "Индекс лога не может быть отрицательным");
        }

        lock (_lock)
        {
            // Индекс, с которого начинаются записи в буфере
            var localIndex = CalculateLocalIndex(startIndex);
            var bufferStartIndex = localIndex - _logStorage.Count;

            if (bufferStartIndex == -1)
            {
                // Надо перезаписать все с самого начала (весь буфер)
                _buffer.Clear();
                _buffer.AddRange(entries);
                return;
            }

            if (bufferStartIndex < -1)
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
    }

    /// <summary>
    /// Добавить в лог одну запись
    /// </summary>
    /// <param name="entry">Запись лога</param>
    /// <returns>Информация о добавленной записи</returns>
    public LogEntryInfo AppendBuffer(LogEntry entry)
    {
        lock (_lock)
        {
            var newIndex = CalculateGlobalIndex(_logStorage.Count + _buffer.Count);
            _buffer.Add(entry);
            return new LogEntryInfo(entry.Term, newIndex);
        }
    }

    private int CalculateGlobalIndex(int localIndex)
    {
        if (SnapshotStorage.HasSnapshot)
        {
            return SnapshotStorage.LastLogEntry.Index
                 + 1 // Учитываем Tomb 
                 + localIndex;
        }

        return localIndex;
    }

    /// <summary>
    /// Содержит ли текущий лог все элементы до указанного индекса включительно
    /// </summary>
    /// <param name="prefix">Информация о записи в другом логе</param>
    /// <returns><c>true</c> - префиксы логов совпадают, <c>false</c> - иначе</returns>
    /// <remarks>Вызывается в AppendEntries для проверки возможности добавления новых записей</remarks>
    public bool PrefixMatch(LogEntryInfo prefix)
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

    private LogEntryInfo GetLogEntryInfoAtIndex(int globalIndex)
    {
        var localIndex = CalculateLocalIndex(globalIndex);
        if (localIndex < -1)
        {
            Serilog.Log.Debug(
                "В логе нет указанного индекса {GlobalIndex}. Локальный индекс: {LocalIndex}. Последний индекс: {LastEntry}",
                globalIndex, localIndex, LastEntry);
            throw new ArgumentOutOfRangeException(nameof(globalIndex), globalIndex, "В логе нет указанного индекса");
        }

        if (localIndex == -1)
        {
            return SnapshotStorage.LastLogEntry;
        }

        if (_logStorage.TryGetInfoAt(localIndex, out var lastLogEntry))
        {
            return lastLogEntry with {Index = globalIndex};
        }

        var bufferEntry = _buffer[localIndex - _logStorage.Count];
        return new LogEntryInfo(bufferEntry.Term, globalIndex);
    }

    /// <summary>
    /// Получить все записи, начиная с указанного индекса
    /// </summary>
    /// <param name="globalIndex">Индекс, начиная с которого нужно вернуть записи</param>
    /// <param name="entries">Хранившиеся записи, начиная с указанного индекса</param>
    /// <returns>Список записей из лога</returns>
    /// <remarks>При выходе за границы, может вернуть пустой массив</remarks>
    /// <remarks>Используется 0-based индекс, а в оригинальном RAFT - 1-based</remarks>
    public bool TryGetFrom(int globalIndex, out IReadOnlyList<LogEntry> entries)
    {
        lock (_lock)
        {
            var localIndex = CalculateLocalIndex(globalIndex);

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
        }
    }

    /// <summary>
    /// Рассчитать локальный индекс по переданному глобальному
    /// </summary>
    /// <param name="globalIndex">Глобальный индекс</param>
    /// <returns>
    /// - Положительное число - локальный индекс среди лога/буфера
    /// - -1 - переданный индекс - это индекс последней команды в снапшоте
    /// - Отрицательное число - команда заходит за пределы снапшота (внутрь)
    /// </returns>
    private int CalculateLocalIndex(int globalIndex)
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
        return CalculateLocalIndexRaw(SnapshotStorage.LastLogEntry.Index, globalIndex);
    }

    private static int CalculateLocalIndexRaw(int snapshotLastIndex, int globalIndex)
    {
        if (snapshotLastIndex == LogEntryInfo.TombIndex)
        {
            return globalIndex;
        }

        return globalIndex - snapshotLastIndex - 1;
    }

    /// <summary>
    /// Закоммитить лог по переданному индексу
    /// </summary>
    /// <param name="index">Индекс новой закомиченной записи</param>
    /// <returns>Результат коммита лога</returns>
    public void Commit(int index)
    {
        if (index < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index,
                "Индекс лога не может быть отрицательным при коммите");
        }

        lock (_lock)
        {
            var localIndex = CalculateLocalIndex(index);
            if (localIndex == -1)
            {
                // Этот индекс указывает на последний индекс из снапшота
                // Такое возможно, когда снапшот только создался и лидер прислал Heartbeat
                return;
            }

            if (localIndex < -1)
            {
                throw new InvalidOperationException(
                    $"Указанный индекс коммита меньше последней команды в снапшоте. Переданный индекс: {index}. Последний индекс снашпота: {SnapshotStorage.LastLogEntry}");
            }

            if (_logStorage.Count + _buffer.Count < localIndex)
            {
                throw new InvalidOperationException(
                    $"Указанный индекс больше максимального хранящегося индекса лога. Переданный индекс: {index}. Последний индекс: {LastEntry.Index}");
            }

            if (localIndex < _logStorage.Count)
            {
                // Нечего коммитить - уже в логе
                return;
            }

            var removeCount = localIndex - _logStorage.Count + 1;
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
    }

    /// <summary>
    /// Получить информацию о записи, предшествующей указанной
    /// </summary>
    /// <param name="nextIndex">Индекс следующей записи</param>
    /// <returns>Информацию о следующей записи в логе</returns>
    /// <remarks>Если указанный индекс 0, то вернется <see cref="LogEntryInfo.Tomb"/></remarks>
    public LogEntryInfo GetPrecedingEntryInfo(int nextIndex)
    {
        if (nextIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс лога не может быть отрицательным");
        }

        if (nextIndex == 0)
        {
            return LogEntryInfo.Tomb;
        }

        lock (_lock)
        {
            if (_snapshotStorage.LastLogEntry.Index + 1 == nextIndex)
            {
                return _snapshotStorage.LastLogEntry;
            }

            return GetLogEntryInfoAtIndex(nextIndex - 1);
        }
    }

    /// <summary>
    /// Получить закомиченные, но еще не примененные записи из лога.
    /// Это записи, индекс которых находится между индексом последней применненной записи (<see cref="LastAppliedGlobalIndex"/>) и
    /// последней закоммиченной записи (<see cref="CommitIndex"/>) 
    /// </summary>
    /// <returns>Записи, которые были закомичены, но еще не применены</returns>
    public IReadOnlyList<LogEntry> GetNotApplied()
    {
        lock (_lock)
        {
            if (LastAppliedGlobalIndex == LogEntryInfo.TombIndex)
            {
                // Никакие записи еще не применялись
                if (_logStorage.Count == 0)
                {
                    return Array.Empty<LogEntry>();
                }

                return _logStorage.ReadFrom(0);
            }

            if (CommitIndex <= LastAppliedGlobalIndex || CommitIndex == LogEntryInfo.TombIndex)
            {
                return Array.Empty<LogEntry>();
            }

            var startIndex = CalculateLocalIndex(LastAppliedGlobalIndex);
            return _logStorage.ReadFrom(startIndex + 1);
        }
    }

    /// <summary>
    /// Указать новый индекс последней примененной записи
    /// </summary>
    /// <param name="globalIndex">Индекс записи в логе</param>
    public void SetLastApplied(int globalIndex)
    {
        if (globalIndex < LogEntryInfo.TombIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(globalIndex), globalIndex,
                "Переданный индекс меньше TombIndex");
        }

        // Индекс не меньше последнего из снапшота
        if (_snapshotStorage.HasSnapshot && globalIndex < _snapshotStorage.LastLogEntry.Index)
        {
            throw new ArgumentOutOfRangeException(nameof(globalIndex), globalIndex,
                "Указанный индекс входит в пределы снапшота");
        }

        LastAppliedGlobalIndex = globalIndex;
    }

    /// <summary>
    /// Перезаписать старый снапшот новым и обновить файл лога (удалить старые записи).
    /// Используется для ответа на InstallSnapshot запрос.
    /// </summary>
    /// <param name="lastIncludedEntry">
    /// Информация о последней примененной команде в <paramref name="snapshot"/>.
    /// Этот метод только сохраняет новый файл снапшота, без очищения или удаления лога команд
    /// </param>
    /// <param name="snapshot">Слепок системы</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>
    /// Поток флагов успешности операции.
    /// Если возвращен <c>false</c> - токен был отменен.
    /// Используется для посылания ответов узлу-лидеру, отправляющему снапшот.
    /// Один <c>true</c> на каждый успешно записанный чанк.
    /// Если достигнут конец, то значение не возвращается - это нужно, чтобы в конце отправить последний InstallSnapshotResponse
    /// </returns>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    /// <remarks>
    /// После установки нового снапшота, лог корректируется - удаляются предшествующие записи
    /// </remarks>
    public IEnumerable<bool> InstallSnapshot(LogEntryInfo lastIncludedEntry,
                                             ISnapshot snapshot,
                                             CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();

        // Сохраним индекс, чтобы потом могли очистить лог
        var lastLocalIndex = CalculateLocalIndex(lastIncludedEntry.Index);

        Debug.Assert(lastLocalIndex >= 0, "Последний индекс в снапшоте меньше индекса первой моей команды в логе",
            "Нельзя установить снапшот, когда последний индекс команды в логе меньше нашей первой команды");

        // 1. Создать временный файл
        var snapshotTempFile = _snapshotStorage.CreateTempSnapshotFile();

        // 2. Записать заголовок: маркер, индекс, терм
        try
        {
            snapshotTempFile.Initialize(lastIncludedEntry);
        }
        catch (Exception e)
        {
            snapshotTempFile.Discard();
            throw;
        }

        yield return true;

        // 3. Записываем сами данные на диск
        foreach (var chunk in snapshot.GetAllChunks(token))
        {
            try
            {
                snapshotTempFile.WriteSnapshotChunk(chunk.Span, token);
            }
            catch (OperationCanceledException)
            {
                snapshotTempFile.Discard();
                throw;
            }
            catch (Exception)
            {
                snapshotTempFile.Discard();
                throw;
            }

            yield return true;
        }

        // 4. Переименовать файл в нужное имя
        snapshotTempFile.Save();

        // 5. Очищаем лог до указанного индекса
        if (lastLocalIndex < _logStorage.Count)
        {
            // Индекс находится в самом файле лога
            if (lastLocalIndex == _logStorage.Count - 1)
            {
                _logStorage.Clear();
            }
            else
            {
                _logStorage.RemoveUntil(lastLocalIndex);
            }
        }
        else if (lastLocalIndex - _logStorage.Count < _buffer.Count)
        {
            var bufferIndex = lastLocalIndex - _logStorage.Count;
            if (bufferIndex == _buffer.Count - 1)
            {
                // Очищаем буфер полностью
                _buffer.Clear();
            }
            else
            {
                // Очищаем буфер до указанного индекса
                _buffer.RemoveRange(0, bufferIndex);
            }

            // Полностью очищаем лог
            _logStorage.Clear();
        }
        else
        {
            // Очищаем лог и буфер
            _logStorage.Clear();
            _buffer.Clear();
        }

        LastAppliedGlobalIndex = lastIncludedEntry.Index;

        /*
         * Ситуации:
         *   - Последний индекс снапшота среди файла лога
         *   - Последний индекс снапшота среди буфера
         *   - Последний индекс снапшота больше всех индексов
         */
    }

    /// <summary>
    /// Метод для создания нового снапшота из существующего состояния при превышении максимального размера лога.
    /// </summary>
    /// <param name="snapshot">Слепок текущего состояния приложения</param>
    /// <param name="token">Токен отмены</param>
    /// <remarks>
    /// В качестве последней команды в файле снапшота будет использоваться последняя примененная команда (<see cref="LastApplied"/>),
    /// поэтому если нужно применить какие-либо команды - делать заранее
    /// </remarks>
    /// <remarks>
    /// Таймер (выборов) не обновляется
    /// </remarks>
    public void SaveSnapshot(ISnapshot snapshot, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var oldLocalIndex = CalculateLocalIndex(LastAppliedGlobalIndex);
        // 1. Создать временный файл
        var snapshotTempFile = _snapshotStorage.CreateTempSnapshotFile();

        try
        {
            snapshotTempFile.Initialize(LastApplied);
        }
        catch (Exception)
        {
            snapshotTempFile.Discard();
            throw;
        }

        // 3. Записываем сами данные на диск
        foreach (var chunk in snapshot.GetAllChunks(token))
        {
            try
            {
                snapshotTempFile.WriteSnapshotChunk(chunk.Span, token);
            }
            catch (OperationCanceledException)
            {
                snapshotTempFile.Discard();
                return;
            }
            catch (Exception)
            {
                snapshotTempFile.Discard();
                throw;
            }
        }

        // 4. Переименовать файл в нужное имя
        lock (_lock)
        {
            try
            {
                snapshotTempFile.Save();
            }
            catch (Exception)
            {
                snapshotTempFile.Discard();
                throw;
            }

            if (_logStorage.Count == 0)
            {
                // Странная ситуация, но ладно
                return;
            }

            if (oldLocalIndex == _logStorage.Count - 1)
            {
                // Если индекс примененной равен последнему, то полностью очищаем
                _logStorage.Clear();
            }
            else
            {
                // Если индекс примененной меньше, то очищаем до указанного индекса
                _logStorage.RemoveUntil(oldLocalIndex);
            }
        }
    }

    /// <summary>
    /// Получить снапшот состояния, если файл существовал
    /// </summary>
    /// <param name="snapshot">Хранившийся файл снапшота</param>
    /// <returns><c>true</c> - файл снапшота существовал, <c>false</c> - файл снапшота не существовал</returns>
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

    /// <summary>
    /// Обновить состояние узла.
    /// Вызывается когда состояние (роль) узла меняется и
    /// нужно обновить голос/терм.
    /// </summary>
    /// <param name="newTerm">Новый терм</param>
    /// <param name="votedFor">Id узла, за который отдали голос</param>
    public void UpdateState(Term newTerm, NodeId? votedFor)
    {
        lock (_lock)
        {
            _metadataStorage.Update(newTerm, votedFor);
        }
    }

    /// <summary>
    /// Проверить превышает файл лога максимальный размер
    /// </summary>
    /// <returns><c>true</c> - размер превышен, <c>false</c> - иначе</returns>
    public bool IsLogFileSizeExceeded()
    {
        return _sizeChecker.IsLogFileSizeExceeded(_logStorage.FileSize);
    }

    // Для тестов
    internal (int LastIndex, Term LastTerm, byte[] SnapshotData) ReadSnapshotFileTest()
    {
        return _snapshotStorage.ReadAllDataTest();
    }

    /// <summary>
    /// Прочитать все записи лога команд: из файла и памяти.
    /// Используется для тестов
    /// </summary>
    /// <returns>Записи лога</returns>
    internal IReadOnlyList<LogEntry> ReadLogFullTest() => _logStorage.ReadAll()       // Файл
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
    internal List<LogEntry> ReadLogFileTest()
    {
        return _logStorage.ReadAllTest()
                          .ToList();
    }

    internal int GetLastAppliedIndexTest() => LastAppliedGlobalIndex;
}