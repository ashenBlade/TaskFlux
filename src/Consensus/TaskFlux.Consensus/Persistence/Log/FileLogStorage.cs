using System.Buffers;
using System.IO.Abstractions;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Consensus.Persistence.Log;

public class FileLogStorage : ILogStorage, IDisposable
{
    private const int Marker = Constants.Marker;

    /// <summary>
    /// Версия-константа для бинарной совместимости.
    /// Вряд-ли будет использоваться, но звучит значимо
    /// </summary>
    private const int CurrentVersion = 1;

    /// <summary>
    /// Общий размер заголовка в байтах: Маркер + Версия
    /// </summary>
    private const int HeaderSizeBytes = 8;

    private const int DataStartPosition = HeaderSizeBytes;

    /// <summary>
    /// Файл лога команд
    /// </summary>
    private readonly IFileInfo _logFile;

    /// <summary>
    /// Поток, представляющий файл
    /// </summary>
    /// <remarks>
    /// Используется базовый <see cref="Stream"/> вместо <see cref="FileStream"/> для тестирования
    /// </remarks>
    private FileSystemStream _fileStream = null!;

    /// <summary>
    /// Директория для временных файлов
    /// </summary>
    private readonly IDirectoryInfo _temporaryDirectory;

    /// <summary>
    /// Список отображений: индекс записи - позиция в файле (потоке)
    /// </summary>
    private List<PositionTerm> _index = null!;

    internal FileLogStorage(IFileInfo logFile, IDirectoryInfo temporaryDirectory)
    {
        _logFile = logFile;
        _temporaryDirectory = temporaryDirectory;

        Initialize();
    }

    /// <summary>
    /// Прочитать и инициализировать индекс с диска.
    /// Выполняется во время создания объекта
    /// </summary>
    private void Initialize()
    {
        if (!_logFile.Exists)
        {
            try
            {
                _fileStream = _logFile.Create();
            }
            catch (UnauthorizedAccessException uae)
            {
                throw new IOException(
                    "Не удалось создать новый файл лога команд для рафта: нет прав для создания файла", uae);
            }
            catch (IOException e)
            {
                throw new IOException("Не удалось создать новый файл лога команд для рафта", e);
            }
        }
        else
        {
            try
            {
                _fileStream = _logFile.Open(FileMode.Open);
            }
            catch (UnauthorizedAccessException uae)
            {
                throw new IOException("Не удалось открыть файл лога команд: ошибка доступа к файлу", uae);
            }
            catch (IOException e)
            {
                throw new IOException("Не удалось открыть файл лога команд: файл уже открыт", e);
            }
        }

        try
        {
            var reader = new StreamBinaryReader(_fileStream);

            if (_fileStream.Length == 0)
            {
                WriteHeader(_fileStream);
                _index = new List<PositionTerm>();
                return;
            }

            if (_fileStream.Length < HeaderSizeBytes)
            {
                throw new InvalidDataException(
                    $"Минимальный размер файла должен быть {HeaderSizeBytes}. Длина файла оказалась {_fileStream.Length}");
            }

            _fileStream.Seek(0, SeekOrigin.Begin);

            // Валидируем заголовок
            var marker = reader.ReadInt32();
            if (marker != Marker)
            {
                throw new InvalidDataException(
                    $"Считанный из файла маркер не равен требуемому. Ожидалось: {Marker}. Получено: {marker}");
            }

            var version = reader.ReadInt32();
            if (CurrentVersion < version)
            {
                throw new InvalidDataException(
                    $"Указанная версия файла больше текущей версии программы. Текущая версия: {CurrentVersion}. Указанная версия: {version}");
            }

            var index = new List<PositionTerm>();

            // Воссоздаем индекс
            try
            {
                long filePosition;
                while (( filePosition = _fileStream.Position ) < _fileStream.Length)
                {
                    var term = reader.ReadInt32();
                    var dataLength = reader.ReadInt32();
                    _fileStream.Seek(dataLength, SeekOrigin.Current);
                    index.Add(new PositionTerm(new Term(term), filePosition));
                }
            }
            catch (EndOfStreamException e)
            {
                throw new InvalidDataException(
                    "Ошибка при воссоздании индекса из файла лога. Не удалось прочитать указанное количество данных",
                    e);
            }

            _index = index;
        }
        catch (Exception)
        {
            _fileStream.Close();
            throw;
        }
    }

    private void WriteHeader(FileSystemStream stream)
    {
        var writer = new StreamBinaryWriter(stream);
        stream.Seek(0, SeekOrigin.Begin);
        writer.Write(Marker);
        writer.Write(CurrentVersion);
        stream.Flush();
    }

    public int Count => _index.Count;

    // Очень надеюсь, что такого никогда не произойдет
    // Чтобы такого точно не произошло, надо на уровне выставления максимального размера файла лога ограничение сделать
    public ulong FileSize => checked( ( ulong ) _fileStream.Length );

    public LogEntryInfo Append(LogEntry entry)
    {
        var savedLastPosition = _fileStream.Seek(0, SeekOrigin.End);
        new StreamBinaryWriter(_fileStream).Write(entry);
        _fileStream.Flush(true);
        _index.Add(new PositionTerm(entry.Term, savedLastPosition));
        return new LogEntryInfo(entry.Term, _index.Count - 1);
    }

    public LogEntryInfo AppendRange(IEnumerable<LogEntry> entries)
    {
        // Вместо поочередной записи используем буффер в памяти.
        // Сначала запишем сериализованные данные на него, одновременно создавая новые записи индекса.
        // После быстро запишем данные на диск и обновим список индексов 

        var entriesArray = entries.ToArray();
        var newIndexes = new List<PositionTerm>(entriesArray.Length);

        var size = CalculateBufferSize(entriesArray);
        var position = _fileStream.Length;
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var writer = new MemoryBinaryWriter(buffer.AsMemory(0, size));
            foreach (var entry in entriesArray)
            {
                var written = writer.Write(entry);
                newIndexes.Add(new PositionTerm(entry.Term, position));
                position += written;
            }

            _fileStream.Seek(0, SeekOrigin.End);
            new StreamBinaryWriter(_fileStream).Write(buffer.AsSpan(0, size));
            _fileStream.Flush(true);

            _index.AddRange(newIndexes);

            return GetLastLogEntryInfoCore();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        int CalculateBufferSize(LogEntry[] logEntries)
        {
            return ( sizeof(int) /* Терм */ + sizeof(int) /* Длина массива */ ) * logEntries.Length
                 + logEntries.Sum(e => e.Data.Length);
        }
    }


    private LogEntryInfo GetLastLogEntryInfoCore()
    {
        return _index.Count == 0
                   ? LogEntryInfo.Tomb
                   : new LogEntryInfo(_index[^1].Term, _index.Count - 1);
    }


    public LogEntryInfo GetPrecedingLogEntryInfo(int nextIndex)
    {
        if (nextIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс записи в логе не может быть отрицательным");
        }

        if (nextIndex > _index.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс записи в логе не может быть больше размера лога");
        }

        return nextIndex == 0
                   ? LogEntryInfo.Tomb
                   : new LogEntryInfo(_index[nextIndex - 1].Term, nextIndex - 1);
    }

    public LogEntryInfo GetLastLogEntry()
    {
        if (_index.Count == 0)
        {
            return LogEntryInfo.Tomb;
        }

        return new LogEntryInfo(_index[^1].Term, _index.Count - 1);
    }

    public IReadOnlyList<LogEntry> ReadAll()
    {
        return ReadLogCore(DataStartPosition, _index.Count);
    }

    public IReadOnlyList<LogEntry> ReadFrom(int startIndex)
    {
        if (_index.Count <= startIndex)
        {
            return Array.Empty<LogEntry>();
        }

        var position = _index[startIndex].Position;
        return ReadLogCore(position, _index.Count - startIndex);
    }

    private IReadOnlyList<LogEntry> ReadLogCore(long position, int countHint)
    {
        var list = new List<LogEntry>(countHint);
        list.AddRange(ReadLogIncrementally(position));
        return list;
    }

    /// <summary>
    /// Читать данные из файла постепенно, начиная с указанной позиции
    /// </summary>
    /// <param name="position">Позиция в файле, с которой нужно читать команды</param>
    /// <returns>Поток записей лога</returns>
    /// <exception cref="InvalidDataException">В файле представлены неверные данные</exception>
    /// <remarks>
    /// Метод будет читать до конца, пока не дойдет до конца файла,
    /// поэтому вызывающий должен контролировать сколько записей он хочет получить
    /// </remarks>
    private IEnumerable<LogEntry> ReadLogIncrementally(long position)
    {
        _fileStream.Seek(position, SeekOrigin.Begin);

        while (_fileStream.Position != _fileStream.Length)
        {
            yield return ReadNextLogEntry();
        }

        LogEntry ReadNextLogEntry()
        {
            try
            {
                var reader = new StreamBinaryReader(_fileStream);
                var term = new Term(reader.ReadInt32());
                var buffer = reader.ReadBuffer();
                return new LogEntry(term, buffer);
            }
            catch (EndOfStreamException e)
            {
                throw new InvalidDataException(
                    "Не удалось прочитать записи команд из файла лога: достигнут конец файла", e);
            }
            catch (ArgumentOutOfRangeException e)
            {
                throw new InvalidDataException("Сериализованное значение терма невалидное", e);
            }
        }
    }

    public LogEntryInfo GetInfoAt(int index)
    {
        return new LogEntryInfo(_index[index].Term, index);
    }

    public bool TryGetInfoAt(int index, out LogEntryInfo info)
    {
        if (index < _index.Count)
        {
            info = new LogEntryInfo(_index[index].Term, index);
            return true;
        }

        info = default!;
        return false;
    }

    public IReadOnlyList<LogEntry> GetRange(int start, int end)
    {
        if (_index.Count < end)
        {
            throw new InvalidOperationException(
                $"Индекс конца больше размера лога. Индекс конца: {end}. Размер лога: {_index.Count}");
        }

        if (end < start)
        {
            throw new ArgumentException(
                $"Индекс конца не может быть раньше индекса начала. Индекс начала: {start}. Индекс конца: {end}");
        }

        // Индексы включительно
        var count = end - start + 1;
        var entries = new List<LogEntry>(count);
        try
        {
            entries.AddRange(ReadLogIncrementally(_index[start].Position).Take(count));
        }
        catch (Exception)
        {
            Serilog.Log.Error("Ошибка на позиции старта {Position} для индекса {Index}", _index[start].Position, start);
            throw;
        }

        return entries;
    }

    public void Clear()
    {
        _fileStream.SetLength(DataStartPosition);
        _index.Clear();
    }

    /// <summary>
    /// Создать новый <see cref="FileLogStorage"/> и тут же его инициализировать.
    /// Испольуется во время старта приложения.
    /// </summary>
    /// <param name="consensusDirectory">
    /// Информация о директории с данными рафта.
    /// В базовой директории - это `{BASE}/consensus`.
    /// В ней должен лежать файл с логом - `raft.log`
    /// </param>
    /// <param name="temporaryDirectory">
    /// Директория для временных файлов.
    /// Должна существовать на момент вызова функции
    /// </param>
    /// <returns>Новый, иницилизированный <see cref="FileLogStorage"/></returns>
    /// <exception cref="InvalidDataException">
    /// Обнаружены ошибки во время инициализации файла (потока) данных: <br/>
    ///    - Поток не пуст и при этом его размер меньше минимального (размер заголовка) <br/> 
    ///    - Полученное магическое число не соответствует требуемому <br/>
    ///    - Указанная в файле версия несовместима с текущей <br/>\
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="temporaryDirectory"/> - не существует
    /// </exception>
    /// <exception cref="IOException">
    /// - Ошибка во время создания нового файла лога, либо <br/>
    /// - Ошибка во время открытия сущесвтующего файла лога, либо <br/>
    /// </exception>
    public static FileLogStorage InitializeFromFileSystem(IDirectoryInfo consensusDirectory,
                                                          IDirectoryInfo temporaryDirectory)
    {
        var fileStream = OpenOrCreateLogFile();
        if (!temporaryDirectory.Exists)
        {
            throw new ArgumentException(
                $"Директории для временных файлов не существует. Директория: {temporaryDirectory.FullName}");
        }

        return new FileLogStorage(fileStream, temporaryDirectory);

        IFileInfo OpenOrCreateLogFile()
        {
            var file = consensusDirectory.FileSystem.FileInfo.New(Path.Combine(consensusDirectory.FullName,
                Constants.LogFileName));
            return file;
        }
    }

    public void Dispose()
    {
        _fileStream.Flush(true);
        _fileStream.Close();
        _fileStream.Dispose();
    }

    /// <summary>
    /// Метод для чтения всех записей из файла снапшота.
    /// </summary>
    /// <remarks>Используется для тестов</remarks>
    internal IEnumerable<LogEntry> ReadAllTest()
    {
        return ReadLogIncrementally(DataStartPosition);
    }

    /// <summary>
    /// Записать все переданные записи в файл.
    /// Метод используется для тестов
    /// </summary>
    internal void SetFileTest(IEnumerable<LogEntry> entries)
    {
        AppendRange(entries);
    }


    /// <summary>
    /// Очистить лог команд до указанного индекса включительно.
    /// После этой операции, из файла лога будут удалены все записи до указанной включительно
    /// </summary>
    /// <param name="index">Индекс последней записи, которую нужно удалить</param>
    public void RemoveUntil(int index)
    {
        if (index == _index.Count - 1)
        {
            // По факту надо очистить весь файл
            _fileStream.SetLength(DataStartPosition);
            _index.Clear();
            return;
        }

        // 1. Создаем временный файл
        var (file, stream) = CreateTempFile();
        try
        {
            // 2. Записываем туда заголовок
            WriteHeader(stream);

            // 3. Копируем данные с исходного файла лога
            var copyStartPosition = _index[index + 1]; // Копировать начинаем со следующего после index
            _fileStream.Seek(copyStartPosition.Position, SeekOrigin.Begin);
            _fileStream.CopyTo(stream);
            stream.Flush(true);

            // 4. Переименовываем
            _fileStream.Close();
            file.MoveTo(_logFile.FullName, true);
            _index.RemoveRange(0, index + 1);
        }
        catch (Exception)
        {
            stream.Close();
            stream.Dispose();
            throw;
        }

        // 5. Обновляем индекс и поток файла лога
        _fileStream.Dispose();
        _fileStream = stream;
        return;

        (IFileInfo, FileSystemStream) CreateTempFile()
        {
            while (true)
            {
                var tempName = Path.GetRandomFileName();
                var fullPath = Path.Combine(_temporaryDirectory.FullName, tempName);
                var tempFile = _temporaryDirectory.FileSystem.FileInfo.New(fullPath);
                try
                {
                    return ( tempFile, tempFile.Open(FileMode.CreateNew) );
                }
                catch (IOException io)
                {
                    Console.WriteLine(io);
                }
            }
        }
    }
}

file static class SerializationHelpers
{
    public static int Write(this ref MemoryBinaryWriter writer, LogEntry entry)
    {
        var written = 0;
        written += writer.Write(entry.Term.Value);
        written += writer.WriteBuffer(entry.Data);
        return written;
    }

    public static void Write(this StreamBinaryWriter writer, LogEntry entry)
    {
        writer.Write(entry.Term.Value);
        writer.WriteBuffer(entry.Data);
    }
}