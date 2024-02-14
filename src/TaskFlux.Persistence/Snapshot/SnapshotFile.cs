using System.Buffers;
using System.ComponentModel;
using System.Diagnostics;
using System.IO.Abstractions;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Persistence.Snapshot;

/// <summary>
/// Файловый интерфейс взаимодействия с файлами снапшотов
/// </summary>
public class SnapshotFile
{
    /// <summary>
    /// Настройки менеджера снапшотов
    /// </summary>
    public SnapshotOptions Options { get; }

    /// <summary>
    /// Маркер файла
    /// </summary>
    private const uint Marker = 0xB6380FC9;

    /// <summary>
    /// Позиция в файле, на которой находится длина данных
    /// </summary>
    private const int LengthPosition = sizeof(uint)  // Маркер
                                     + sizeof(long)  // Последний индекс
                                     + sizeof(long); // Последний терм

    /// <summary>
    /// Позиция в файле, на которой начинаются сами данные
    /// </summary>
    private const long SnapshotDataStartPosition = LengthPosition + sizeof(int);

    private readonly IFileInfo _snapshotFile;
    private readonly IDirectoryInfo _temporarySnapshotFileDirectory;

    public bool HasSnapshot => _snapshotInfo.HasValue;

    private SnapshotFile(
        IFileInfo snapshotFile,
        IDirectoryInfo temporarySnapshotFileDirectory,
        (LogEntryInfo LastApplied, int DataLength, uint CheckSum)? snapshotInfo,
        SnapshotOptions options)
    {
        ArgumentNullException.ThrowIfNull(snapshotFile);
        ArgumentNullException.ThrowIfNull(temporarySnapshotFileDirectory);
        ArgumentNullException.ThrowIfNull(options);

        Options = options;
        _snapshotFile = snapshotFile;
        _temporarySnapshotFileDirectory = temporarySnapshotFileDirectory;
        _snapshotInfo = snapshotInfo;
    }

    public ISnapshotFileWriter CreateTempSnapshotFile()
    {
        return new FileSystemSnapshotFileWriter(this);
    }

    /// <summary>
    /// Информация о последней записи лога, которая была применена к снапшоту
    /// </summary>
    /// <remarks><see cref="LogEntryInfo.Tomb"/> - означает отсутствие снапшота</remarks>
    public LogEntryInfo LastApplied => _snapshotInfo is var (lastApplied, _, _)
                                           ? lastApplied
                                           : LogEntryInfo.Tomb;

    /// <summary>
    /// Данные о файле снапшота.
    /// Не null, если файл снапшота существует
    /// </summary>
    private (LogEntryInfo LastApplied, int DataLength, uint CheckSum)? _snapshotInfo;

    public bool TryGetSnapshot(out ISnapshot snapshot, out LogEntryInfo lastApplied)
    {
        lastApplied = LastApplied;

        if (lastApplied.IsTomb)
        {
            snapshot = default!;
            return false;
        }

        snapshot = new FileSystemSnapshot(this);
        return true;
    }

    public bool TryGetIncludedIndex(out Lsn lastIncluded)
    {
        if (HasSnapshot)
        {
            lastIncluded = LastApplied.Index;
            return true;
        }

        lastIncluded = default;
        return false;
    }

    public bool TryGetLastEntryInfo(out LogEntryInfo lastIncludedEntryInfo)
    {
        if (HasSnapshot)
        {
            lastIncludedEntryInfo = LastApplied;
            return true;
        }

        lastIncludedEntryInfo = LogEntryInfo.Tomb;
        return false;
    }

    private class FileSystemSnapshot : ISnapshot
    {
        private readonly SnapshotFile _parent;

        public FileSystemSnapshot(SnapshotFile parent)
        {
            _parent = parent;
        }

        public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
        {
            if (_parent._snapshotInfo is not var (_, length, _))
            {
                yield break;
            }

            using var stream = _parent._snapshotFile.OpenRead();

            stream.Seek(SnapshotDataStartPosition, SeekOrigin.Begin);
            var buffer = ArrayPool<byte>.Shared.Rent(Environment.SystemPageSize);
            try
            {
                var left = length;
                while (0 < left)
                {
                    var currentLength = Math.Min(buffer.Length, left);
                    var span = buffer.AsSpan(0, currentLength);
                    stream.ReadExactly(span);
                    yield return buffer.AsMemory(0, currentLength);
                    left -= currentLength;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    private class FileSystemSnapshotFileWriter : ISnapshotFileWriter
    {
        /// <summary>
        /// Временный файл снапшота
        /// </summary>
        private IFileInfo? _temporarySnapshotFile;

        /// <summary>
        /// Поток нового файла снапшота.
        /// Создается во время вызова <see cref="Initialize"/>
        /// </summary>
        private FileSystemStream? _temporarySnapshotFileStream;

        /// <summary>
        /// Значение LogEntry, которое было записано в файл снапшота
        /// </summary>
        private LogEntryInfo? _writtenLogEntry;

        /// <summary>
        /// Родительский объект хранилища снапшота.
        /// Для него это все и замутили - ему нужно обновить файл снапшота
        /// </summary>
        private SnapshotFile _parent;

        /// <summary>
        /// Чек-сумма для сохраняемого снапшота.
        /// Обновляется с установкой каждого чанка снапшота.
        /// </summary>
        private uint _checkSum = Crc32CheckSum.InitialValue;

        /// <summary>
        /// Длина сохраняемого снапшота.
        /// Обновляется с каждым записанным чанком.
        /// </summary>
        private int _length;

        private enum SnapshotFileState
        {
            /// <summary>
            /// Только начинается работа
            /// </summary>
            Start = 0,

            /// <summary>
            /// Записан заголовок
            /// </summary>
            Initialized = 1,

            /// <summary>
            /// Работа с файлом закончена (вызваны <see cref="FileSystemSnapshotFileWriter.Save"/> или <see cref="FileSystemSnapshotFileWriter.Discard"/>
            /// </summary>
            Finished = 2,
        }

        /// <summary>
        /// Состояние файла снапшота во время его создания (записи в него)
        /// </summary>
        private SnapshotFileState _state = SnapshotFileState.Start;

        public FileSystemSnapshotFileWriter(SnapshotFile parent)
        {
            _parent = parent;
        }

        public void Initialize(LogEntryInfo lastIncluded)
        {
            switch (_state)
            {
                case SnapshotFileState.Initialized:
                    throw new InvalidOperationException(
                        "Повторная попытка записать заголовок файла снапшота: файл уже инициализирован");
                case SnapshotFileState.Finished:
                    throw new InvalidOperationException(
                        "Повторная попытка записать заголовок файла снапшота: работа с файлом уже завершена");
                case SnapshotFileState.Start:
                    break;
                default:
                    throw new InvalidEnumArgumentException(nameof(_state), ( int ) _state, typeof(SnapshotFileState));
            }

            // 1. Создаем новый файл снапшота
            var (file, stream) = CreateAndOpenTemporarySnapshotFile();

            var writer = new StreamBinaryWriter(stream);

            // 2. Записываем маркер файла
            writer.Write(Marker);

            // 3. Записываем заголовок основной информации
            writer.Write(lastIncluded.Index);
            writer.Write(lastIncluded.Term);

            // 4. Пока длина не известна
            writer.Write(_length); // == writer.Write(0);

            _writtenLogEntry = lastIncluded;
            _temporarySnapshotFileStream = stream;
            _temporarySnapshotFile = file;

            _state = SnapshotFileState.Initialized;
        }

        private (IFileInfo File, FileSystemStream Stream) CreateAndOpenTemporarySnapshotFile()
        {
            var tempDir = _parent._temporarySnapshotFileDirectory;
            while (true)
            {
                var file = tempDir.FileSystem.FileInfo.New(GetRandomTempFileName());
                try
                {
                    var stream = file.Open(FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                    return ( file, stream );
                }
                catch (IOException)
                {
                }
            }

            string GetRandomTempFileName()
            {
                var fileName = Path.GetRandomFileName();
                return Path.Combine(tempDir.FullName, fileName);
            }
        }

        public void Save()
        {
            switch (_state)
            {
                case SnapshotFileState.Start:
                    throw new InvalidOperationException("Попытка сохранения неинициализированного файла снапшота");
                case SnapshotFileState.Finished:
                    throw new InvalidOperationException(
                        "Нельзя повторно сохранить файл снапшота: работа с файлом закончена");
                case SnapshotFileState.Initialized:
                    break;
                default:
                    throw new InvalidEnumArgumentException(nameof(_state), ( int ) _state, typeof(SnapshotFileState));
            }

            Debug.Assert(_temporarySnapshotFileStream is not null, "Поток временного файла не должен быть null");
            Debug.Assert(_temporarySnapshotFile is not null, "Объект временного файла не должен быть null");
            Debug.Assert(_writtenLogEntry is not null,
                "Объект информации последней команды снапшота не должен быть null");

            var writer = new StreamBinaryWriter(_temporarySnapshotFileStream);
            writer.Write(_checkSum);
            _temporarySnapshotFileStream.Seek(LengthPosition, SeekOrigin.Begin);
            writer.Write(_length);

            _temporarySnapshotFileStream.Fsync();
            _temporarySnapshotFileStream.Close();
            _temporarySnapshotFileStream.Dispose();

            _temporarySnapshotFile.MoveTo(_parent._snapshotFile.FullName, true);

            _parent._snapshotInfo = ( _writtenLogEntry.Value, _length, _checkSum );

            _state = SnapshotFileState.Finished;
        }

        public void Discard()
        {
            switch (_state)
            {
                case SnapshotFileState.Finished:
                    return;
                case SnapshotFileState.Start:
                case SnapshotFileState.Initialized:
                    break;
                default:
                    throw new InvalidEnumArgumentException(nameof(_state), ( int ) _state, typeof(SnapshotFileState));
            }

            _state = SnapshotFileState.Finished;

            _temporarySnapshotFileStream?.Close();
            _temporarySnapshotFileStream = null;

            _temporarySnapshotFile?.Delete();
            _temporarySnapshotFile = null;

            _parent = null!;
        }

        public void WriteSnapshotChunk(ReadOnlySpan<byte> chunk, CancellationToken token)
        {
            switch (_state)
            {
                case SnapshotFileState.Start:
                    throw new InvalidOperationException("Попытка записи снапшота в неинициализированный файл");
                case SnapshotFileState.Finished:
                    throw new InvalidOperationException(
                        "Нельзя записывать данные в файл снапшота: работа с файлом закончена");
                case SnapshotFileState.Initialized:
                    break;
                default:
                    Debug.Assert(false, $"Неизвестное состояние при записи файла снапшота: {_state}");
                    throw new InvalidEnumArgumentException(nameof(_state), ( int ) _state, typeof(SnapshotFileState));
            }

            Debug.Assert(_temporarySnapshotFileStream is not null, "Поток файла снапшота не должен быть null");
            _temporarySnapshotFileStream.Write(chunk);
            _length += chunk.Length;
            _checkSum = Crc32CheckSum.Compute(_checkSum, chunk);
        }
    }

    /// <summary>
    /// Проинициализировать файл и получить информацию о последней включенной в снапшот команде
    /// </summary>
    /// <param name="file">Файл снапшота</param>
    /// <returns>Информация о последней включенной в файл команде</returns>
    private static (LogEntryInfo LastEntry, int Length, uint CheckSum)? Initialize(IFileInfo file)
    {
        if (!file.Exists || file.Length == 0)
        {
            return null;
        }

        using var stream = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        const int minHeaderSize = sizeof(uint)  // Маркер 
                                + sizeof(long)  // Индекс
                                + sizeof(long); // Терм

        if (stream.Length < minHeaderSize)
        {
            throw new InvalidDataException(
                $"Размер файла не пуст и его размер меньше минимального. Минимальный размер: {minHeaderSize}. Размер файла: {stream.Length}");
        }

        stream.Seek(0, SeekOrigin.Begin);
        var reader = new StreamBinaryReader(stream);

        // 1. Маркер
        var marker = reader.ReadUInt32();
        if (marker != Marker)
        {
            throw new InvalidDataException(
                $"Хранившийся в файле маркер не равен требуемому. Прочитано: {marker}. Ожидалось: {Marker}");
        }

        var index = reader.ReadLsn();
        var term = reader.ReadTerm();

        var length = reader.ReadInt32();
        if (length < 0)
        {
            throw new InvalidDataException("Длина данных не может быть отрицательной");
        }

        uint storedCheckSum;
        if (length == 0)
        {
            storedCheckSum = reader.ReadUInt32();
            if (storedCheckSum != Crc32CheckSum.InitialValue)
            {
                throw new InvalidDataException(
                    $"Снапшот отсутствует, но прочитанная чек-сумма не равна сохраненной. Прочитанная: {storedCheckSum}. Ожидаемая: {Crc32CheckSum.InitialValue}");
            }

            return ( new LogEntryInfo(term, index), 0, storedCheckSum );
        }


        var computedCheckSum = Crc32CheckSum.InitialValue;

        Span<byte> buffer = stackalloc byte[Environment.SystemPageSize];
        var left = length;
        do
        {
            var span = buffer[..Math.Min(buffer.Length, left)];
            stream.ReadExactly(span);
            computedCheckSum = Crc32CheckSum.Compute(computedCheckSum, span);
            left -= span.Length;
        } while (left > 0);

        storedCheckSum = reader.ReadUInt32();
        if (storedCheckSum != computedCheckSum)
        {
            throw new InvalidDataException(
                $"Рассчитанная чек-сумма не равна сохраненной. Рассчитанная: {computedCheckSum}. Сохраненная: {storedCheckSum}");
        }

        return ( new LogEntryInfo(term, index), length, storedCheckSum );
    }

    public static SnapshotFile Initialize(IDirectoryInfo dataDirectory, SnapshotOptions options)
    {
        var snapshotFile =
            dataDirectory.FileSystem.FileInfo.New(Path.Combine(dataDirectory.FullName, Constants.SnapshotFileName));
        var tempDirectory = dataDirectory.FileSystem.DirectoryInfo.New(Path.Combine(dataDirectory.FullName,
            Constants.TemporaryDirectoryName));

        if (!tempDirectory.Exists)
        {
            try
            {
                tempDirectory.Create();
            }
            catch (IOException e)
            {
                throw new InvalidDataException("Ошибка при создании директории для временных файлов", e);
            }
        }

        try
        {
            var info = Initialize(snapshotFile);
            return new SnapshotFile(snapshotFile, tempDirectory, info, options);
        }
        catch (EndOfStreamException e)
        {
            throw new InvalidDataException("Файл снапшота меньше чем ожидалось", e);
        }
    }


    // Для тестов
    internal (Lsn LastIndex, Term LastTerm, byte[] SnapshotData) ReadAllDataTest()
    {
        using var stream = _snapshotFile.OpenRead();
        var reader = new StreamBinaryReader(stream);

        var marker = reader.ReadUInt32();
        Debug.Assert(marker == Marker, "marker == Marker", "Маркер файла должен равняться константному значению");

        var index = reader.ReadLsn();
        var term = reader.ReadTerm();

        // Читаем до конца
        var data = reader.ReadBuffer();
        var computedCheckSum = Crc32CheckSum.Compute(data);
        var storedCheckSum = reader.ReadUInt32();
        if (computedCheckSum != storedCheckSum)
        {
            throw new InvalidDataException(
                $"Прочитанная чек-сумма не равна вычисленной. Прочитанная чек-сумма: {storedCheckSum}. Рассчитанная: {computedCheckSum}");
        }

        return ( index, term, data );
    }

    /// <summary>
    /// Записать в файл снапшота тестовые данные
    /// </summary>
    /// <param name="lastTerm">Терм последней команды снапшота</param>
    /// <param name="lastIndex">Индекс последней команды снапшота</param>
    /// <param name="snapshot">Снапшот, который нужно записать</param>
    internal void SetupSnapshotTest(Term lastTerm, Lsn lastIndex, ISnapshot snapshot)
    {
        using var stream = _snapshotFile.OpenWrite();
        var writer = new StreamBinaryWriter(stream);

        writer.Write(Marker);
        writer.Write(lastIndex);
        writer.Write(lastTerm);

        var data = ReadSnapshotBytes();
        writer.WriteBuffer(data);
        var checkSum = Crc32CheckSum.Compute(data);
        writer.Write(checkSum);

        _snapshotInfo = ( new LogEntryInfo(lastTerm, lastIndex), data.Length, checkSum );
        return;

        byte[] ReadSnapshotBytes()
        {
            var memory = new MemoryStream();
            foreach (var chunk in snapshot.GetAllChunks())
            {
                memory.Write(chunk.Span);
            }

            return memory.ToArray();
        }
    }

    /// <summary>
    /// Получить размер файла снапшота в байтах.
    /// Используется для метрик
    /// </summary>
    /// <returns>Размер файла снапшота в байтах</returns>
    internal long CalculateSnapshotFileSize()
    {
        if (!_snapshotFile.Exists)
        {
            return 0;
        }

        try
        {
            return _snapshotFile.Length;
        }
        catch (IOException)
        {
            return 0;
        }
    }
}