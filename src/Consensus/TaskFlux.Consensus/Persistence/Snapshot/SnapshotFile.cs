using System.Buffers;
using System.ComponentModel;
using System.Diagnostics;
using System.IO.Abstractions;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Persistence.Snapshot;

/// <summary>
/// Файловый интерфейс взаимодействия с файлами снапшотов
/// </summary>
public class SnapshotFile
{
    /// <summary>
    /// Маркер файла
    /// </summary>
    private const uint Marker = 0xB6380FC9;

    private readonly IFileInfo _snapshotFile;
    private readonly IDirectoryInfo _temporarySnapshotFileDirectory;

    public bool HasSnapshot => !LastApplied.IsTomb;

    private SnapshotFile(
        IFileInfo snapshotFile,
        IDirectoryInfo temporarySnapshotFileDirectory,
        LogEntryInfo lastApplied,
        int length)
    {
        ArgumentNullException.ThrowIfNull(snapshotFile);
        ArgumentNullException.ThrowIfNull(temporarySnapshotFileDirectory);
        _snapshotFile = snapshotFile;
        _temporarySnapshotFileDirectory = temporarySnapshotFileDirectory;
        LastApplied = lastApplied;
        SnapshotLength = length;
    }

    public ISnapshotFileWriter CreateTempSnapshotFile()
    {
        return new FileSystemSnapshotFileWriter(this);
    }

    /// <summary>
    /// Информация о последней записи лога, которая была применена к снапшоту
    /// </summary>
    /// <remarks><see cref="LogEntryInfo.Tomb"/> - означает отсутствие снапшота</remarks>
    public LogEntryInfo LastApplied { get; private set; }

    /// <summary>
    /// Размер снапшота
    /// </summary>
    public int SnapshotLength { get; private set; }

    public bool TryGetSnapshot(out ISnapshot snapshot, out LogEntryInfo lastApplied)
    {
        lastApplied = LastApplied;
        if (lastApplied.IsTomb)
        {
            snapshot = default!;
            return false;
        }

        snapshot = new FileSystemSnapshot(_snapshotFile);
        return true;
    }

    private const long SnapshotDataStartPosition = sizeof(int)  // Маркер
                                                 + sizeof(int)  // Терм
                                                 + sizeof(int); // Индекс

    private class FileSystemSnapshot : ISnapshot
    {
        private const int BufferSize = 4 * 1024; // 4 Кб (размер страницы)

        private readonly IFileInfo _snapshotFile;

        public FileSystemSnapshot(IFileInfo snapshotFile)
        {
            _snapshotFile = snapshotFile;
        }

        public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
        {
            using var stream = _snapshotFile.OpenRead();
            stream.Seek(SnapshotDataStartPosition, SeekOrigin.Begin);
            var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
            try
            {
                int read;
                while (( read = stream.Read(buffer) ) != 0)
                {
                    yield return buffer.AsMemory(0, read);
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
        private Stream? _temporarySnapshotFileStream;

        /// <summary>
        /// Значение LogEntry, которое было записано в файл снапшота
        /// </summary>
        private LogEntryInfo? _writtenLogEntry;

        /// <summary>
        /// Родительский объект хранилища снапшота.
        /// Для него это все и замутили - ему нужно обновить файл снапшота
        /// </summary>
        private SnapshotFile _parent;

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
            writer.Write(lastIncluded.Term.Value);

            _writtenLogEntry = lastIncluded;
            _temporarySnapshotFileStream = stream;
            _temporarySnapshotFile = file;

            _state = SnapshotFileState.Initialized;
        }

        private (IFileInfo File, Stream Stream) CreateAndOpenTemporarySnapshotFile()
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

            _temporarySnapshotFileStream.Flush();
            _temporarySnapshotFileStream.Close();
            _temporarySnapshotFileStream.Dispose();
            _temporarySnapshotFile.MoveTo(_parent._snapshotFile.FullName, true);

            _parent.LastApplied = _writtenLogEntry.Value;

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
        }
    }

    /// <summary>
    /// Проинициализировать файл и получить информацию о последней включенной в снапшот команде
    /// </summary>
    /// <param name="file">Файл снапшота</param>
    /// <returns>Информация о последней включенной в файл команде</returns>
    private static (LogEntryInfo LastEntry, int Length) Initialize(IFileInfo file)
    {
        if (!file.Exists || file.Length == 0)
        {
            return ( LogEntryInfo.Tomb, 0 );
        }

        using var stream = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        const int minHeaderSize = sizeof(int)  // Маркер 
                                + sizeof(int)  // Индекс
                                + sizeof(int); // Терм

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

        var index = reader.ReadInt32();
        if (index < 0)
        {
            throw new InvalidDataException($"Индекс команды, хранившийся в снапшоте, - отрицательный. Индекс: {index}");
        }

        var term = reader.ReadInt32();
        if (term < Term.StartTerm)
        {
            throw new InvalidDataException($"Терм команды, хранившийся в снапшоте, - отрицательный. Терм: {term}");
        }

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

            return ( new LogEntryInfo(term, index), 0 );
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

        return ( new LogEntryInfo(new Term(term), index), length );
    }

    public static SnapshotFile Initialize(IDirectoryInfo dataDirectory)
    {
        var snapshotFile =
            dataDirectory.FileSystem.FileInfo.New(Path.Combine(dataDirectory.FullName, Constants.SnapshotFileName));
        var tempDirectory =
            dataDirectory.FileSystem.DirectoryInfo.New(Path.Combine(dataDirectory.FullName,
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
            var (lastEntry, length) = Initialize(snapshotFile);
            return new SnapshotFile(snapshotFile, tempDirectory, lastEntry, length);
        }
        catch (EndOfStreamException e)
        {
            throw new InvalidDataException("Файл снапшота меньше чем ожидалось", e);
        }
    }


    // Для тестов
    internal (int LastIndex, Term LastTerm, byte[] SnapshotData) ReadAllDataTest()
    {
        using var stream = _snapshotFile.OpenRead();
        var reader = new StreamBinaryReader(stream);

        var marker = reader.ReadUInt32();
        Debug.Assert(marker == Marker);

        var index = reader.ReadInt32();
        var term = new Term(reader.ReadInt32());

        // Читаем до конца
        var memory = new MemoryStream();
        stream.CopyTo(memory);
        return ( index, term, memory.ToArray() );
    }

    /// <summary>
    /// Записать в файл снапшота тестовые данные
    /// </summary>
    /// <param name="lastTerm">Терм последней команды снапшота</param>
    /// <param name="lastIndex">Индекс последней команды снапшота</param>
    /// <param name="snapshot">Снапшот, который нужно записать</param>
    internal void SetupSnapshotTest(Term lastTerm, int lastIndex, ISnapshot snapshot)
    {
        using var stream = _snapshotFile.OpenWrite();
        var writer = new StreamBinaryWriter(stream);

        writer.Write(Marker);
        writer.Write(lastIndex);
        writer.Write(lastTerm.Value);
        foreach (var chunk in snapshot.GetAllChunks())
        {
            writer.Write(chunk.Span);
        }

        LastApplied = new LogEntryInfo(lastTerm, lastIndex);
    }
}