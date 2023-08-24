using System.Buffers;
using System.IO.Abstractions;
using Consensus.Core;
using Consensus.Core.Log;
using Consensus.Storage.File;
using Consensus.Storage.File.Log;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Persistence.Log;

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
    /// Поток, представляющий файл
    /// </summary>
    /// <remarks>
    /// Используется базовый <see cref="Stream"/> вместо <see cref="FileStream"/> для тестирования
    /// </remarks>
    private readonly Stream _fileStream;

    private StreamBinaryReader _reader;
    private StreamBinaryWriter _writer;

    /// <summary>
    /// Список отображений: индекс записи - позиция в файле (потоке)
    /// </summary>
    private List<PositionTerm> _index = null!;

    internal FileLogStorage(Stream fileStream)
    {
        if (!fileStream.CanRead)
        {
            throw new ArgumentException("Переданный поток не поддерживает чтение", nameof(fileStream));
        }

        if (!fileStream.CanSeek)
        {
            throw new ArgumentException("Переданный поток не поддерживает позиционирование", nameof(fileStream));
        }

        if (!fileStream.CanWrite)
        {
            throw new ArgumentException("Переданный поток не поддерживает запись", nameof(fileStream));
        }

        _fileStream = fileStream;

        _writer = new StreamBinaryWriter(fileStream);
        _reader = new StreamBinaryReader(fileStream);

        Initialize();
    }

    /// <summary>
    /// Прочитать и инициализировать индекс с диска.
    /// Выполняется во время создания объекта
    /// </summary>
    private void Initialize()
    {
        if (_fileStream.Length == 0)
        {
            _fileStream.Seek(0, SeekOrigin.Begin);
            _writer.Write(Marker);
            _writer.Write(CurrentVersion);
            // _writer.Flush();

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
        var marker = _reader.ReadInt32();
        if (marker != Marker)
        {
            throw new InvalidDataException(
                $"Считанный из файла маркер не равен требуемому. Ожидалось: {Marker}. Получено: {marker}");
        }

        var version = _reader.ReadInt32();
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
                var term = _reader.ReadInt32();
                var dataLength = _reader.ReadInt32();
                _fileStream.Seek(dataLength, SeekOrigin.Current);
                index.Add(new PositionTerm(new Term(term), filePosition));
            }
        }
        catch (EndOfStreamException e)
        {
            throw new InvalidDataException(
                "Ошибка при воссоздании индекса из файла лога. Не удалось прочитать указанное количество данных", e);
        }

        _index = index;
    }

    public int Count => _index.Count;

    // Очень надеюсь, что такого никогда не произойдет
    // Чтобы такого точно не произошло, надо на уровне выставления максимального размера файла лога ограничение сделать
    public ulong Size => checked( ( ulong ) _fileStream.Length );

    public LogEntryInfo Append(LogEntry entry)
    {
        var savedLastPosition = _fileStream.Seek(0, SeekOrigin.End);
        _writer.Write(entry);
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
            _writer.Write(buffer.AsSpan(0, size));
            _writer.Flush();

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
                var term = new Term(_reader.ReadInt32());
                var buffer = _reader.ReadBuffer();
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

    public void ClearCommandLog()
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
    /// <returns>Новый, иницилизированный <see cref="FileLogStorage"/></returns>
    /// <exception cref="InvalidDataException">
    /// Обнаружены ошибки во время инициализации файла (потока) данных: <br/>
    ///    - Поток не пуст и при этом его размер меньше минимального (размер заголовка) <br/> 
    ///    - Полученное магическое число не соответствует требуемому <br/>
    ///    - Указанная в файле версия несовместима с текущей <br/>\
    /// </exception>
    /// <exception cref="IOException">
    /// - Ошибка во время создания нового файла лога, либо <br/>
    /// - Ошибка во время открытия сущесвтующего файла лога, либо <br/>
    /// </exception>
    public static FileLogStorage InitializeFromFileSystem(IDirectoryInfo consensusDirectory)
    {
        var fileStream = OpenOrCreateLogFile();

        return new FileLogStorage(fileStream);

        FileSystemStream OpenOrCreateLogFile()
        {
            var file = consensusDirectory.FileSystem.FileInfo.New(Path.Combine(consensusDirectory.FullName,
                Constants.LogFileName));
            FileSystemStream stream;

            if (!file.Exists)
            {
                try
                {
                    stream = file.Create();
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
                    stream = file.Open(FileMode.Open);
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

            return stream;
        }
    }

    public void Dispose()
    {
        _fileStream.Flush();
        _fileStream.Close();
        _fileStream.Dispose();
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