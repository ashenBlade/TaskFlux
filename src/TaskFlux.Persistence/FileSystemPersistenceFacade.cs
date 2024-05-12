using System.IO.Abstractions;
using System.Runtime.CompilerServices;
using Serilog;
using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Persistence.Log;
using TaskFlux.Persistence.Metadata;
using TaskFlux.Persistence.Snapshot;
using Constants = TaskFlux.Consensus.Persistence.Constants;

[assembly: InternalsVisibleTo("Consensus.Storage.Tests")]

namespace TaskFlux.Persistence;

public class FileSystemPersistenceFacade : IPersistence, IDisposable
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
    private readonly SegmentedFileLog _log;

    /// <summary>
    /// Файл метаданных - `consensus/raft.metadata`
    /// </summary>
    private readonly MetadataFile _metadata;

    /// <summary>
    /// Файл снапшота - `consensus/raft.snapshot`
    /// </summary>
    private readonly SnapshotFile _snapshot;

    public LogEntryInfo LastEntry => _log.TryGetLastLogEntry(out var lastLog)
        ? lastLog
        : _snapshot.TryGetLastEntryInfo(out var snapshotEntry)
            ? snapshotEntry
            : throw new InvalidDataException(
                "Не удалось получить данные о последней записи");

    public Lsn CommitIndex => _log.CommitIndex;
    public Term CurrentTerm => _metadata.Term;
    public NodeId? VotedFor => _metadata.VotedFor;
    public SnapshotFile Snapshot => _snapshot;
    public SegmentedFileLog Log => _log;
    public MetadataFile Metadata => _metadata;

    internal FileSystemPersistenceFacade(SegmentedFileLog log,
        MetadataFile metadata,
        SnapshotFile snapshot,
        ILogger logger)
    {
        _log = log;
        _metadata = metadata;
        _snapshot = snapshot;
        _logger = logger;
    }

    public static FileSystemPersistenceFacade Initialize(IDirectoryInfo dataDirectory,
        ILogger logger,
        SnapshotOptions snapshotOptions,
        SegmentedFileLogOptions logOptions)
    {
        CheckDataDirectory();
        var metadata = InitializeMetadata();
        var log = InitializeLog();
        var snapshot = InitializeSnapshot();
        CheckSnapshotAndLogInterrelation();
        return new FileSystemPersistenceFacade(log, metadata, snapshot, logger);

        void CheckDataDirectory()
        {
            if (!dataDirectory.Exists)
            {
                logger.Information("Директории для данных {DirectoryPath} не обнаружено. Создаю новую",
                    dataDirectory.FullName);
                dataDirectory.Create();
            }
        }

        void CheckSnapshotAndLogInterrelation()
        {
            if (!snapshot.TryGetIncludedIndex(out var lastSnapshotIndex))
            {
                // Если снапшота нет, то лог обрезан быть не может.
                // Т.е. начинаться должен с самого начала - индекса 0
                if (log.StartIndex != 0)
                {
                    throw new InvalidDataException(
                        "Снапшота не обнаружено и лог начинается не с 0 индекса. Восстановление состояния невозможно");
                }

                return;
            }

            // В противном случае, необходимо проверить где индекс снапшота лежит
            if (lastSnapshotIndex == log.StartIndex - 1)
            {
                // Нормальная ситуация - создали снапшот и удалили сегменты
                return;
            }

            if (lastSnapshotIndex < log.StartIndex)
            {
                throw new InvalidDataException(
                    $"Индекс последней записи в снапшоте гораздо меньше начального индекса лога. "
                    + $"Индекс снапшота {lastSnapshotIndex}. "
                    + $"Начальный индекс лога {log.StartIndex}. "
                    + $"Состояние восстановить невозможно");
            }

            if (log.LastRecordIndex < lastSnapshotIndex)
            {
                // Снапшот содержит более актуальные данные, чем последняя запись в логе.
                // Такое может быть возможно, когда лог был поврежден и поврежденные сегменты были удалены.
                // Работать с логом в этом случае нельзя, поэтому просто удаляем все текущие записи и начинаем новый сегмент
                logger.Warning(
                    "Индекс снапшота ({SnapshotLastIndex}) больше последнего индекса в логе ({LogLastIndex}). Удаляю текущие сегменты лога и начинаю новый",
                    lastSnapshotIndex, log.LastRecordIndex);
                log.StartNewWith(lastSnapshotIndex + 1);
            }
            else
            {
                // В противном случае, индекс снапшота находится где-то в логе.
                // Нам необходимо закоммитить этот индекс, так как перезапись приведет к потере данных
                // (команды из снапшота получить обратно не можем) 
                log.Commit(lastSnapshotIndex);
            }
        }

        SnapshotFile InitializeSnapshot()
        {
            var snapshotFile =
                dataDirectory.FileSystem.FileInfo.New(Path.Combine(dataDirectory.FullName, Constants.SnapshotFileName));
            if (!snapshotFile.Exists)
            {
                logger.Information("Файл снапшота не обнаружен. Создаю новый - {Path}", snapshotFile.FullName);
                try
                {
                    // Сразу закроем
                    using var __ = snapshotFile.Create();
                }
                catch (Exception e)
                {
                    logger.Fatal(e, "Ошибка при создании файла снапшота - {Path}", snapshotFile.FullName);
                    throw;
                }
            }

            return SnapshotFile.Initialize(dataDirectory, snapshotOptions);
        }

        SegmentedFileLog InitializeLog()
        {
            try
            {
                // ReSharper disable once ContextualLoggerProblem
                return SegmentedFileLog.Initialize(dataDirectory, logger, logOptions);
            }
            catch (Exception e)
            {
                logger.Fatal(e, "Ошибка во время инициализации файла лога");
                throw;
            }
        }

        MetadataFile InitializeMetadata()
        {
            try
            {
                return MetadataFile.Initialize(dataDirectory);
            }
            catch (InvalidDataException invalidDataException)
            {
                logger.Fatal(invalidDataException, "Переданный файл метаданных был в невалидном состоянии");
                throw;
            }
            catch (Exception e)
            {
                logger.Fatal(e, "Ошибка во время инициализации файла метаданных");
                throw;
            }
        }
    }

    public bool IsUpToDate(LogEntryInfo prefix)
    {
        // Неважно на каком индексе последний элемент.
        // Если он последний, то наши старые могут быть заменены

        // Наш:    | 1 | 1 | 2 | 3 |
        // Другой: | 1 | 1 | 2 | 3 | 4 | 5 |
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
            return true;
        }

        return false;
    }

    public void InsertRange(IReadOnlyList<LogEntry> entries, Lsn startIndex)
    {
        if (entries.Count == 0)
        {
            return;
        }

        _logger.Verbose("Записываю {Count} записей по индексу {GlobalIndex}", entries.Count, startIndex);
        _log.InsertRangeOverwrite(entries, startIndex);
    }

    public Lsn Append(LogEntry entry)
    {
        _logger.Verbose("Добавляю запись {Entry} в лог", entry);
        return _log.Append(entry);
    }

    public bool PrefixMatch(LogEntryInfo prefix)
    {
        if (_log.TryGetLogEntryInfo(prefix.Index, out var storedLogEntry))
        {
            return storedLogEntry.Term == prefix.Term;
        }

        if (prefix.IsTomb)
        {
            /*
             * Эта ситуация может возникнуть, когда:
             * 1. Лог пуст (кластер только что запущен)
             * 2. Лидер отправил AppendEntries и индекс предыдущей записи равен -1 (Tomb)
             */
            return _log.StartIndex == 0;
        }

        return false;
    }

    public bool TryGetFrom(Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevLogEntry)
    {
        return _log.TryGetFrom(index, out entries, out prevLogEntry);
    }

    public void Commit(Lsn index)
    {
        _log.Commit(index);
    }

    /// <summary>
    /// Установить индекс коммита в указанное значение
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Указанный индекс либо меньше индекса из снапшота, либо больше последнего индекса в логе</exception>
    internal void SetCommitTest(Lsn commit)
    {
        _log.SetCommitIndexTest(commit);
    }

    /// <summary>
    /// Выставить в файле снапшота указанные данные
    /// </summary>
    /// <param name="term">Терм последней записи</param>
    /// <param name="lastIncludedIndex">Индекс последней записи</param>
    /// <param name="snapshot">Содержимое снапшота</param>
    private void SetupSnapshotTest(Term term, Lsn lastIncludedIndex, ISnapshot snapshot)
    {
        _snapshot.SetupSnapshotTest(term, lastIncludedIndex, snapshot);
    }

    /// <summary>
    /// Установить состояние данных в указанное.
    /// Вызывать нужно этот метод для корректной установки состояния с обновлением нужных полей
    /// </summary>
    /// <param name="logEntries">Записи в логе</param>
    /// <param name="snapshotData">Данные для снапшота</param>
    /// <param name="commitIndex">Индекс коммита</param>
    /// <param name="startLsn">Начальный индекс в логе</param>
    internal void SetupTest(IReadOnlyList<LogEntry>? logEntries = null,
        (Term term, Lsn lastIncludedIndex, ISnapshot snapshot)? snapshotData = null,
        Lsn? commitIndex = null,
        Lsn? startLsn = null)
    {
        SetupTest(logEntries: logEntries is not null
                ? (logEntries, Array.Empty<IReadOnlyList<LogEntry>>())
                : null,
            snapshotData,
            commitIndex,
            startLsn);
    }

    internal void SetupTest(
        (IReadOnlyList<LogEntry> tail, IReadOnlyList<IReadOnlyList<LogEntry>> sealedSegments)? logEntries = null,
        (Term term, Lsn lastIncludedIndex, ISnapshot snapshot)? snapshotData = null,
        Lsn? commitIndex = null,
        Lsn? startLsn = null,
        (Term, NodeId?)? metadata = null)
    {
        if (logEntries is var (tail, s))
        {
            _log.SetupLogTest(tail, s, startLsn);
        }
        else if (startLsn.HasValue)
        {
            _log.SetupLogTest(Array.Empty<LogEntry>(), Array.Empty<IReadOnlyList<LogEntry>>(), startLsn);
        }

        if (snapshotData is var (term, index, snapshot))
        {
            SetupSnapshotTest(term, index, snapshot);
        }

        if (metadata is var (currentTerm, votedFor))
        {
            _metadata.SetupMetadataTest(currentTerm, votedFor);
        }

        if (commitIndex is { } ci)
        {
            SetCommitTest(ci);
        }
        else if (snapshotData is var (_, snapshotIndex, _))
        {
            SetCommitTest(snapshotIndex);
        }
    }

    public LogEntryInfo GetEntryInfo(Lsn index)
    {
        return _log.GetEntryInfoAt(index);
    }

    public bool TryGetSnapshotLastEntryInfo(out LogEntryInfo entryInfo)
    {
        entryInfo = Snapshot.LastApplied;
        return !entryInfo.IsTomb;
    }

    public IEnumerable<byte[]> ReadDeltaFromPreviousSnapshot()
    {
        if (_log.Count == 0)
        {
            yield break;
        }

        IEnumerable<byte[]> range;
        if (_snapshot.TryGetIncludedIndex(out var index))
        {
            var startIndex = index + 1;

            // Может возникнуть ситуация, когда индекс снапшота равен последнему в логе
            if (_log.LastRecordIndex < startIndex)
            {
                range = Enumerable.Empty<byte[]>();
            }
            else
            {
                range = _log.ReadDataRange(startIndex, _log.LastRecordIndex);
            }
        }
        else
        {
            range = _log.ReadDataRange(_log.StartIndex, _log.LastRecordIndex);
        }

        foreach (var delta in range)
        {
            yield return delta;
        }
    }

    public ISnapshotInstaller CreateSnapshot(LogEntryInfo lastIncludedSnapshotEntry)
    {
        // Вся работа будет весить через временный файл снапшота.
        // Для записи в этом файл будет использоваться обертка/фасад
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

        return new FileSystemSnapshotInstaller(snapshotTempFile, lastIncludedSnapshotEntry.Index, this);
    }

    private class FileSystemSnapshotInstaller : ISnapshotInstaller
    {
        private readonly ISnapshotFileWriter _snapshotFileWriter;
        private readonly Lsn _lastIndex;
        private readonly FileSystemPersistenceFacade _parent;

        public FileSystemSnapshotInstaller(ISnapshotFileWriter snapshotFileWriter,
            Lsn lastIndex,
            FileSystemPersistenceFacade parent)
        {
            _snapshotFileWriter = snapshotFileWriter;
            _lastIndex = lastIndex;
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

            // 2. Освобождаем лог 
            _parent._log.DeleteCoveredSegmentsUntil(_lastIndex);
        }

        public void Discard()
        {
            _snapshotFileWriter.Discard();
        }
    }

    public bool TryGetSnapshot(out ISnapshot snapshot, out LogEntryInfo lastLogEntry)
    {
        return Snapshot.TryGetSnapshot(out snapshot, out lastLogEntry);
    }

    public void UpdateState(Term newTerm, NodeId? votedFor)
    {
        lock (_lock)
        {
            _logger.Verbose("Записываю новый терм {Term} и отданный голос {VotedFor}", newTerm, votedFor);
            _metadata.Update(newTerm, votedFor);
        }
    }

    /// <summary>
    /// Следует ли создавать новый снапшот приложения
    /// </summary>
    public bool ShouldCreateSnapshot()
    {
        /*
         * Новый снапшот следует создавать в случае, если количество сегментов между
         * - тем, что содержит индекс снапшота
         * - и тем, в котором находится индекс коммита
         * превышает определенное количество.
         *
         * Мы учитываем общее количество сегментов, т.к. после создания снапшота сегменты, покрываемые снапшотом удаляются.
         */

        var uncoveredSegments = _log.GetSegmentsBefore(_log.CommitIndex);
        return _snapshot.Options.SnapshotCreationSegmentsThreshold <= uncoveredSegments;
    }

    public IEnumerable<byte[]> ReadCommittedDeltaFromPreviousSnapshot()
    {
        lock (_lock)
        {
            var startIndex = Snapshot.TryGetIncludedIndex(out var s)
                ? s + 1
                : _log.StartIndex;

            var endIndex = CommitIndex;

            if (endIndex < startIndex)
            {
                /*
                 * Вообще, такого в нормально состоянии случиться не может, но возможная ситуация:
                 * - Приложение только запустилось
                 * - Снапшот есть
                 * - Лог есть, но его индекс начала меньше индекса снапшота
                 * - Индекс коммита равен первому в логе
                 */
                yield break;
            }

            foreach (var bytes in _log.ReadDataRange(startIndex, endIndex))
            {
                yield return bytes;
            }
        }
    }

    public void Dispose()
    {
        _log.Dispose();
        _metadata.Dispose();
    }
}