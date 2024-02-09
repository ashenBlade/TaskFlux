using System.Diagnostics;
using System.IO.Abstractions;
using System.Runtime.CompilerServices;
using Serilog;
using TaskFlux.Consensus.Persistence.Log;
using TaskFlux.Consensus.Persistence.Snapshot;
using TaskFlux.Core;
using TaskFlux.Persistence.Metadata;
using TaskFlux.Persistence.Snapshot;

[assembly: InternalsVisibleTo("Consensus.Storage.Tests")]

namespace TaskFlux.Consensus.Persistence;

public class FileSystemPersistenceFacade : IPersistence
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

    public LogEntryInfo LastEntry => _log.GetLastLogEntry();
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

    public static FileSystemPersistenceFacade Initialize(string? dataDirectoryPath, ulong maxFileLogFile)
    {
        // TODO: пробрасывать настройки в аргументах Initialize 
        // TODO: инициализировать корректно индекс последней записи
        var logger = Serilog.Log.ForContext<FileSystemPersistenceFacade>();
        var dataDirPath = GetDataDirectory();
        var fs = new FileSystem();
        var dataDirectory = CreateDataDirectory();
        var dataDirectoryInfo = new DirectoryInfoWrapper(fs, dataDirectory);
        var fileLogStorage = CreateFileLogStorage();
        var metadataStorage = CreateMetadataStorage();
        var snapshotStorage = CreateSnapshotStorage(new DirectoryInfoWrapper(fs, dataDirectory));

        return new FileSystemPersistenceFacade(fileLogStorage,
            metadataStorage,
            snapshotStorage,
            logger);

        DirectoryInfo CreateDataDirectory()
        {
            var dir = new DirectoryInfo(Path.Combine(dataDirPath, "data"));
            if (!dir.Exists)
            {
                logger.Information("Директории для хранения данных не существует. Создаю новую - {Path}",
                    dir.FullName);
                try
                {
                    dir.Create();
                }
                catch (IOException e)
                {
                    logger.Fatal(e, "Невозможно создать директорию для данных");
                    throw;
                }
            }

            return dir;
        }

        SnapshotFile CreateSnapshotStorage(IDirectoryInfo dataDir)
        {
            var snapshotFile = new FileInfo(Path.Combine(dataDirectory.FullName, "raft.snapshot"));
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

            return SnapshotFile.Initialize(dataDir);
        }

        SegmentedFileLog CreateFileLogStorage()
        {
            try
            {
                // ReSharper disable once ContextualLoggerProblem
                return SegmentedFileLog.Initialize(dataDirectoryInfo, logger.ForContext<SegmentedFileLog>());
            }
            catch (Exception e)
            {
                logger.Fatal(e, "Ошибка во время инициализации файла лога");
                throw;
            }
        }

        MetadataFile CreateMetadataStorage()
        {
            try
            {
                return MetadataFile.Initialize(dataDirectoryInfo);
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

        string GetDataDirectory()
        {
            string workingDirectory;
            if (!string.IsNullOrWhiteSpace(dataDirectoryPath))
            {
                workingDirectory = dataDirectoryPath;
                logger.Debug("Указана рабочая директория: {WorkingDirectory}", workingDirectory);
            }
            else
            {
                var currentDirectory = Directory.GetCurrentDirectory();
                logger.Information("Директория данных не указана. Выставляю в рабочую директорию: {CurrentDirectory}",
                    currentDirectory);
                workingDirectory = currentDirectory;
            }

            return workingDirectory;
        }
    }

    public bool IsUpToDate(LogEntryInfo prefix)
    {
        // ссылка на etcd: https://github.com/etcd-io/raft/blob/main/log.go#L435
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

        lock (_lock)
        {
            _log.InsertRangeOverwrite(entries, startIndex);
        }
    }

    public Lsn Append(LogEntry entry)
    {
        lock (_lock)
        {
            _logger.Verbose("Добавляю запись {Entry} в лог", entry);
            return _log.Append(entry);
        }
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
    internal void SetupTest(IReadOnlyList<LogEntry>? logEntries = null,
                            (Term term, Lsn lastIncludedIndex, ISnapshot snapshot)? snapshotData = null,
                            int? commitIndex = null)
    {
        if (logEntries is not null)
        {
            SetupLogTest(logEntries);
        }

        if (snapshotData is var (term, index, snapshot))
        {
            SetupSnapshotTest(term, index, snapshot);
        }

        if (commitIndex is { } ci)
        {
            SetCommitTest(ci);
        }
    }

    private void SetupLogTest(IReadOnlyList<LogEntry> logEntries)
    {
        if (_snapshot.TryGetIncludedIndex(out var snapshotIndex))
        {
            Debug.Assert(snapshotIndex < logEntries.Count, "snapshotIndex < logEntries.Count",
                "Количество записей в логе не может быть меньше индекса последней команды в снапшоте");
        }

        _log.SetupLogTest(logEntries);
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
            _parent._log.DeleteUntil(_lastIndex);
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
        return _snapshot.Options.SegmentsBeforeSnapshot <= uncoveredSegments;
    }


    public IEnumerable<byte[]> ReadCommittedDeltaFromPreviousSnapshot()
    {
        lock (_lock)
        {
            if (CommitIndex.IsTomb)
            {
                yield break;
            }

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
}