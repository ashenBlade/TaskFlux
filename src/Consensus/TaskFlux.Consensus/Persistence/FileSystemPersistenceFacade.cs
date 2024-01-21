using System.Runtime.CompilerServices;
using Serilog;
using TaskFlux.Consensus.Persistence.Log;
using TaskFlux.Consensus.Persistence.LogFileCheckStrategy;
using TaskFlux.Consensus.Persistence.Metadata;
using TaskFlux.Consensus.Persistence.Snapshot;
using TaskFlux.Core;

[assembly: InternalsVisibleTo("Consensus.Storage.Tests")]

namespace TaskFlux.Consensus.Persistence;

public class FileSystemPersistenceFacade
{
    private readonly ILogger _logger;

    /// <summary>
    /// Объект для синхронизации работы.
    /// Необходима для синхронизации работы между обработчиками узлов (когда лидер),
    /// или когда параллельно будут приходить запросы от других узлов
    /// </summary>
    private readonly object _lock = new();

    /// <summary>
    /// Файл лога команд - `consensus/raft.log`
    /// </summary>
    private readonly FileLog _log;

    /// <summary>
    /// Файл метаданных - `consensus/raft.metadata`
    /// </summary>
    private readonly MetadataFile _metadata;

    /// <summary>
    /// Файл снапшота - `consensus/raft.snapshot`
    /// </summary>
    private readonly SnapshotFile _snapshot;

    /// <summary>
    /// Метод для проверки превышения размера файла лога.
    /// Подобная логика нужна для тестов
    /// </summary>
    private readonly ILogFileSizeChecker _sizeChecker;

    /// <summary>
    /// Последняя запись в логе, включая незакоммиченные записи и запись в снапшоте.
    /// </summary>
    public LogEntryInfo LastEntry
    {
        get
        {
            lock (_lock)
            {
                if (_snapshot.HasSnapshot)
                {
                    if (_log.TryGetLastLogEntry(out var logLastEntry))
                    {
                        return logLastEntry with
                        {
                            Index = logLastEntry.Index
                                  + _snapshot.LastApplied.Index
                                  + 1
                        };
                    }

                    return _snapshot.LastApplied;
                }


                return _log.GetLastLogEntry();
            }
        }
    }

    /// <summary>
    /// Индекс последней закоммиченной записи
    /// </summary>
    public int CommitIndex
    {
        get
        {
            lock (_lock)
            {
                if (Snapshot.HasSnapshot)
                {
                    if (_log.TryGetCommitIndex(out var logCommitIndex))
                    {
                        return Snapshot.LastApplied.Index + logCommitIndex + 1;
                    }

                    return Snapshot.LastApplied.Index;
                }

                return _log.CommitIndex;
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
                return _metadata.Term;
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
                return _metadata.VotedFor;
            }
        }
    }

    public SnapshotFile Snapshot => _snapshot;
    public FileLog Log => _log;
    public MetadataFile Metadata => _metadata;

    public FileSystemPersistenceFacade(FileLog log,
                                       MetadataFile metadata,
                                       SnapshotFile snapshot,
                                       ILogger logger,
                                       ulong maxLogFileSize = Constants.MaxLogFileSize)
        : this(log, metadata, snapshot, logger, new SizeLogFileSizeChecker(maxLogFileSize))
    {
    }

    internal FileSystemPersistenceFacade(FileLog log,
                                         MetadataFile metadata,
                                         SnapshotFile snapshot,
                                         ILogger logger,
                                         ILogFileSizeChecker sizeChecker)
    {
        _log = log;
        _metadata = metadata;
        _snapshot = snapshot;
        _logger = logger;
        _sizeChecker = sizeChecker;
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
    /// Возможно затирание старых записей.
    /// </summary>
    /// <param name="entries">Записи, которые необходимо добавить</param>
    /// <param name="startIndex">Индекс, начиная с которого необходимо добавить записи (включительно)</param>
    public void InsertRange(IReadOnlyList<LogEntry> entries, int startIndex)
    {
        if (startIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(startIndex), startIndex,
                "Индекс лога не может быть отрицательным");
        }

        if (entries.Count == 0)
        {
            return;
        }

        lock (_lock)
        {
            // Индекс, с которого начинаются записи в логе
            var logIndex = CalculateLogIndex(startIndex);
            _logger.Verbose("Записываю {Count} записей по индексу {GlobalIndex}/{LogIndex}", entries.Count, startIndex,
                logIndex);
            // Добавляем записи в файл
            _log.InsertRangeOverwrite(entries, logIndex);
        }
    }

    /// <summary>
    /// Добавить в лог одну запись
    /// </summary>
    /// <param name="entry">Запись лога</param>
    /// <returns>Информация о добавленной записи</returns>
    public LogEntryInfo Append(LogEntry entry)
    {
        lock (_lock)
        {
            _logger.Verbose("Добавляю запись {Entry} в лог", entry);
            var logInfo = _log.Append(entry);
            return logInfo with {Index = CalculateGlobalIndex(_snapshot.LastApplied.Index, logInfo.Index)};
        }
    }

    private static int CalculateGlobalIndex(int snapshotIndex, int logIndex)
    {
        if (snapshotIndex == LogEntryInfo.TombIndex)
        {
            return logIndex;
        }

        // Индекс лога не нужно проверять на Tomb, т.к. даже если он и Tomb, то +1 его поглотит
        return snapshotIndex
             + logIndex
             + 1;
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

        var localIndex = CalculateLogIndex(prefix.Index);

        if (localIndex < -1)
        {
            /*
             * Префикс указывает на запись, которая находится в нашем снапшоте.
             * Так как это снапшот, то все записи были закоммичены и последняя включенная запись,
             * должна иметь терм не меньше терма переданного префикса.
             * Это единственное предположение.
             */
            return prefix.Term <= Snapshot.LastApplied.Term;
        }

        if (localIndex == -1)
        {
            // Префикс указывает на последнюю запись в снапшоте
            return prefix.Term == Snapshot.LastApplied.Term;
        }

        if (_log.TryGetLogEntryInfo(localIndex, out var entryInfo))
        {
            // Префикс указывает на запись, которая хранится в логе
            return entryInfo.Term == prefix.Term;
        }

        // Префикса с указанной записью у нас нет
        return false;
    }

    private LogEntryInfo GetLogEntryInfoAtIndex(int globalIndex)
    {
        var localIndex = CalculateLogIndex(globalIndex);
        if (localIndex < -1)
        {
            throw new ArgumentOutOfRangeException(nameof(globalIndex), globalIndex, "В логе нет указанного индекса");
        }

        if (localIndex == -1)
        {
            return Snapshot.LastApplied;
        }

        return _log.GetInfoAt(localIndex) with {Index = globalIndex};
    }

    /// <summary>
    /// Получить все записи, начиная с указанного индекса
    /// </summary>
    /// <param name="globalIndex">Индекс, начиная с которого нужно вернуть записи</param>
    /// <param name="entries">Хранившиеся записи, начиная с указанного индекса</param>
    /// <returns>Список записей из лога</returns>
    /// <remarks>При выходе за границы, может вернуть пустой массив</remarks>
    public bool TryGetFrom(int globalIndex, out IReadOnlyList<LogEntry> entries)
    {
        lock (_lock)
        {
            var localIndex = CalculateLogIndex(globalIndex);

            if (localIndex < 0)
            {
                entries = Array.Empty<LogEntry>();
                return false;
            }

            entries = _log.GetFrom(localIndex);
            return true;
        }
    }

    /// <summary>
    /// Рассчитать локальный индекс по переданному глобальному
    /// </summary>
    /// <param name="globalIndex">Глобальный индекс</param>
    /// <returns>
    /// - Неотрицательное (>= 0) - индекс записи в логе (локальный)
    /// - -1 - это индекс последней команды в снапшоте
    /// - Отрицательное число - команда заходит за пределы снапшота (внутрь)
    /// </returns>
    private int CalculateLogIndex(int globalIndex)
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

        // Если снапшота нет, то -(-1) - 1 = 0 - ничего не изменятся (индекс лога == индекс глобальный)
        // Если снапшот есть, то из переданного globalIndex вычитается кол-во записей в снапшоте = -(LastAppliedIndex + 1) 
        return globalIndex - ( Snapshot.LastApplied.Index + 1 );
    }

    /// <summary>
    /// Закоммитить лог по переданному индексу
    /// </summary>
    /// <param name="index">Индекс новой закоммиченной записи</param>
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
            var logIndex = CalculateLogIndex(index);
            if (logIndex == -1)
            {
                // Этот индекс указывает на последний индекс из снапшота
                // Такое возможно, когда снапшот только создался и лидер прислал Heartbeat
                return;
            }

            if (logIndex < -1)
            {
                throw new InvalidOperationException(
                    $"Указанный индекс коммита меньше последней команды в снапшоте. Переданный индекс: {index}. Последний индекс снапшота: {Snapshot.LastApplied}");
            }

            _logger.Verbose("Коммичу индекс {GlobalIndex}/{LogIndex}", index, logIndex);
            _log.Commit(logIndex);
        }
    }

    /// <summary>
    /// Получить информацию о записи по указанному индексу
    /// </summary>
    /// <param name="index">Индекс записи</param>
    /// <returns>Информация о записи для которой нужно получить информацию</returns>
    /// <exception cref="NotImplementedException"></exception>
    public LogEntryInfo GetEntryInfo(int index)
    {
        if (index < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index, "Индекс записи не может быть отрицательным");
        }

        lock (_lock)
        {
            var localIndex = CalculateLogIndex(index);
            if (localIndex == -1)
            {
                return Snapshot.LastApplied;
            }

            var logEntryInfo = _log.GetInfoAt(localIndex);

            if (Snapshot.LastApplied.IsTomb)
            {
                return logEntryInfo;
            }

            return logEntryInfo with {Index = logEntryInfo.Index + Snapshot.LastApplied.Index};
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
            if (_snapshot.LastApplied.Index + 1 == nextIndex)
            {
                return _snapshot.LastApplied;
            }

            return GetLogEntryInfoAtIndex(nextIndex - 1);
        }
    }

    public bool TryGetSnapshotLastEntryInfo(out LogEntryInfo entryInfo)
    {
        entryInfo = Snapshot.LastApplied;
        return !entryInfo.IsTomb;
    }

    /// <summary>
    /// Создать объект для записи нового снапшота
    /// </summary>
    /// <param name="lastIncludedSnapshotEntry">Последняя включенная запись в снапшот</param>
    /// <returns>Объект для создания нового снапшота</returns>
    public ISnapshotInstaller CreateSnapshot(LogEntryInfo lastIncludedSnapshotEntry)
    {
        // Вся работа будет весить через временный файл снапшота.
        // Для записи в этом файл будет использоваться обертка/фасад
        var lastLocalIndex = CalculateLogIndex(lastIncludedSnapshotEntry.Index);
        var snapshotTempFile = _snapshot.CreateTempSnapshotFile();
        try
        {
            _logger.Verbose("Инициализирую временный файл снапшота с последней записью {LastEntry}",
                lastIncludedSnapshotEntry);
            snapshotTempFile.Initialize(lastIncludedSnapshotEntry);
        }
        catch (Exception)
        {
            snapshotTempFile.Discard();
            throw;
        }

        return new FileSystemSnapshotInstaller(lastLocalIndex, snapshotTempFile, this);
    }

    private class FileSystemSnapshotInstaller : ISnapshotInstaller
    {
        private readonly int _lastIncludedLocalIndex;
        private readonly ISnapshotFileWriter _snapshotFileWriter;
        private readonly FileSystemPersistenceFacade _parent;

        public FileSystemSnapshotInstaller(
            int lastIncludedLocalIndex,
            ISnapshotFileWriter snapshotFileWriter,
            FileSystemPersistenceFacade parent)
        {
            _lastIncludedLocalIndex = lastIncludedLocalIndex;
            _snapshotFileWriter = snapshotFileWriter;
            _parent = parent;
        }

        public void InstallChunk(ReadOnlySpan<byte> chunk, CancellationToken token)
        {
            _snapshotFileWriter.WriteSnapshotChunk(chunk, token);
        }

        public void Commit()
        {
            _parent._logger.Verbose("Сохраняю файл снапшота");
            // 1. Обновляем файл снапшота
            _snapshotFileWriter.Save();

            // 2. Очищаем лог
            if (_lastIncludedLocalIndex >= 0)
            {
                _parent._logger.Verbose("Очищаю лог до индекса {LogIndex}", _lastIncludedLocalIndex);
                _parent.Log.TruncateUntil(_lastIncludedLocalIndex);
            }
            else
            {
                _parent._logger.Verbose("Очищаю лог полностью");
                _parent.Log.Clear();
            }
        }

        public void Discard()
        {
            _snapshotFileWriter.Discard();
        }
    }

    /// <summary>
    /// Метод для создания нового снапшота из существующего состояния при превышении максимального размера лога.
    /// </summary>
    /// <param name="snapshot">Слепок текущего состояния приложения</param>
    /// <param name="lastIncludedEntry">Последняя включенная в снапшот запись</param>
    /// <param name="token">Токен отмены</param>
    /// <remarks>
    /// Таймер (выборов) не обновляется
    /// </remarks>
    public void SaveSnapshot(ISnapshot snapshot, LogEntryInfo lastIncludedEntry, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var oldLocalIndex = CalculateLogIndex(lastIncludedEntry.Index);
        // 1. Создать временный файл
        var snapshotTempFile = _snapshot.CreateTempSnapshotFile();

        try
        {
            snapshotTempFile.Initialize(lastIncludedEntry);
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

        // 4. Обновляем данные
        lock (_lock)
        {
            try
            {
                _logger.Verbose("Обновляю файл снапшота");
                snapshotTempFile.Save();
            }
            catch (Exception)
            {
                snapshotTempFile.Discard();
                throw;
            }

            if (oldLocalIndex >= 0)
            {
                _logger.Verbose("Очищаю файл лога до индекса {LogIndex}", oldLocalIndex);
                _log.TruncateUntil(oldLocalIndex);
            }
            else
            {
                _logger.Verbose("Очищаю файл лога полностью");
                _log.Clear();
            }
        }
    }

    /// <summary>
    /// Получить снапшот состояния, если файл существовал
    /// </summary>
    /// <param name="snapshot">Хранившийся файл снапшота</param>
    /// <param name="lastLogEntry">Последняя запись, включенная в снапшот</param>
    /// <returns><c>true</c> - файл снапшота существовал, <c>false</c> - файл снапшота не существовал</returns>
    public bool TryGetSnapshot(out ISnapshot snapshot, out LogEntryInfo lastLogEntry)
    {
        return Snapshot.TryGetSnapshot(out snapshot, out lastLogEntry);
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
            _logger.Verbose("Записываю новый терм {Term} и отданный голос {VotedFor}", newTerm, votedFor);
            _metadata.Update(newTerm, votedFor);
        }
    }

    /// <summary>
    /// Проверить превышает файл лога максимальный размер
    /// </summary>
    /// <returns><c>true</c> - размер превышен, <c>false</c> - иначе</returns>
    public bool ShouldCreateSnapshot()
    {
        return _sizeChecker.IsLogFileSizeExceeded(_log.FileSize);
    }

    /// <summary>
    /// Прочитать из лога все закоммиченные команды.
    /// </summary>
    /// <returns>Список из всех закоммиченных команд</returns>
    public IEnumerable<byte[]> ReadCommittedDelta()
    {
        lock (_lock)
        {
            foreach (var bytes in _log.ReadCommittedData())
            {
                yield return bytes;
            }
        }
    }
}