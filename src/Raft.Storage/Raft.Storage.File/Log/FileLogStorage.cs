using System.Runtime.CompilerServices;
using System.Text;
using Raft.Core;
using Raft.Core.Log;

namespace Raft.Storage.File;

public class FileLogStorage: ILogStorage
{
    private const int Marker = Constants.Marker;

    /// <summary>
    /// Версия-константа для бинарной совместимости.
    /// Вряд-ли будет использоваться, но выглядит значимо
    /// </summary>
    private const int CurrentVersion = 1;

    /// <summary>
    /// Общий размер заголовка: Маркер + Версия
    /// </summary>
    private const int HeaderSizeBytes = 8;

    private const int DataStartPosition = HeaderSizeBytes;
    
    /// <summary>
    /// Кодировка, используемая для сериализации/десериализации команды
    /// </summary>
    private static readonly Encoding Encoding = Encoding.UTF8;
    
    /// <summary>
    /// Поток, представляющий файл
    /// </summary>
    /// <remarks>
    /// Используется базовый <see cref="Stream"/> вместо <see cref="FileStream"/> для тестирования
    /// </remarks>
    private readonly Stream _file;

    private readonly BinaryReader _reader;
    private readonly BinaryWriter _writer;

    /// <summary>
    /// Список отображений: индекс записи - позиция в файле (потоке)
    /// </summary>
    private List<PositionTerm>? _index;

    /// <summary>
    /// Флаг инициализации
    /// </summary>
    private bool _initialized;
    
    public FileLogStorage(Stream file)
    {
        if (!file.CanRead)
        {
            throw new ArgumentException("Переданный поток не поддерживает чтение", nameof(file));
        }

        if (!file.CanSeek)
        {
            throw new ArgumentException("Переданный поток не поддерживает позиционирование", nameof(file));
        }

        if (!file.CanWrite)
        {
            throw new ArgumentException("Переданный поток не поддерживает запись", nameof(file));
        }
        
        _file = file;
        _writer = new BinaryWriter(file, Encoding, true);
        _reader = new BinaryReader(file, Encoding, true);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckInitialized()
    {
        if (!_initialized)
        {
            Initialize();
        }
    }

    /// <summary>
    /// Проверить и инициализировать поток
    /// </summary>
    private void Initialize()
    {
        if (_file.Length == 0)
        {
            _file.Seek(0, SeekOrigin.Begin);
            _writer.Write(Marker);
            _writer.Write(CurrentVersion);
            _writer.Flush();
            
            _index = new List<PositionTerm>();
            _initialized = true;
            return;
        }
        
        if (_file.Length < HeaderSizeBytes)
        {
            throw new InvalidDataException(
                $"Минимальный размер файла должен быть {HeaderSizeBytes}. Длина файла оказалась {_file.Length}");
        }

        _file.Seek(0, SeekOrigin.Begin);
        
        // Валидируем заголовок
        var marker = _reader.ReadInt32();
        if (marker != Marker)
        {
            throw new InvalidDataException($"Считанный из файла маркер не равен требуемому. Ожидалось: {Marker}. Получено: {marker}");
        }

        var version = _reader.ReadInt32();
        if (CurrentVersion < version)
        {
            throw new InvalidDataException(
                $"Указанная версия файла меньше текущей версии программы. Текущая версия: {CurrentVersion}. Указанная версия: {version}");
        }

        var index = new List<PositionTerm>();
        
        // Воссоздаем индекс
        try
        {
            while (_reader.PeekChar() != -1)
            {
                var position = _file.Position;
                var term = _reader.ReadInt32();
                var stringLength = _reader.Read7BitEncodedInt();
                _file.Seek(stringLength, SeekOrigin.Current);
                index.Add(new PositionTerm(new Term(term), position));
            }
        }
        catch (EndOfStreamException e)
        {
            throw new InvalidDataException("Ошибка при воссоздании индекса из файла лога. Не удалось прочитать указанное количество данных", e);
        }

        _index = index;
        _initialized = true;
    }

    public LogEntryInfo Append(LogEntry entry)
    {
        CheckInitialized();

        var savedLastPosition = _file.Seek(0, SeekOrigin.End);
        try
        {
            Serialize(entry, _writer);
            _writer.Flush();
        }
        catch (Exception)
        {
            try { _file.Position = savedLastPosition; } catch (Exception) { /* */ }
            throw;
        }
        
        _index!.Add(new PositionTerm(entry.Term, savedLastPosition));
        return new LogEntryInfo(entry.Term, _index.Count - 1);
    }

    public LogEntryInfo AppendRange(IEnumerable<LogEntry> entries, int index)
    {
        CheckInitialized();
        
        if (_index!.Count < index)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index, $"Индекс для вставки записи превысил наибольший хранимый индекс {_index.Count}");
        }

        // Вместо поочередной записи используем буффер в памяти.
        // Сначала запишем сериализованные данные на него, одновременно создавая новые записи индекса.
        // После быстро запишем данные на диск и обновим список индексов 

        var entriesArray = entries.ToArray();
        
        var newIndexes = new List<PositionTerm>();
        using var memory = new MemoryStream(( sizeof(int) + 128 ) * entriesArray.Length);
        using var writer = new BinaryWriter(memory, Encoding, true);
        var oldLength = _file.Length;
        
        var startPosition = index == _index!.Count
                                ? _file.Length
                                : _index[index].Position;

        foreach (var entry in entriesArray)
        {
            var currentPosition = startPosition + memory.Position;
            writer.Write(entry.Term.Value);
            writer.Write(entry.Data);
            newIndexes.Add(new PositionTerm(entry.Term, currentPosition));
        }

        if (index == _index.Count)
        {
            _file.Seek(0, SeekOrigin.End);
        }
        else
        {
            _file.Seek(startPosition, SeekOrigin.Begin);
        }
        
        var dataBytes = memory.ToArray();
        _writer.Write(dataBytes);
        _writer.Flush();
        
        // Текущая позиция в файле находится в самом конце актуальных данных
        // Если не обрезать файл, то будут читаться мусорные байты
        // Потом надо заменить на специальный маркер
        if (_file.Position < oldLength)
        {
            _file.SetLength(_file.Position);
        }
            
        _index!.RemoveRange(index, _index.Count - index);
        _index.AddRange(newIndexes);

        return GetLastLogEntryInfoCore();
    }

    private LogEntryInfo GetLastLogEntryInfoCore()
    {
        CheckInitialized();
        
        return _index!.Count == 0
                   ? LogEntryInfo.Tomb
                   : new LogEntryInfo(_index[^1].Term, _index.Count - 1);
    }
    

    public LogEntryInfo GetPrecedingLogEntryInfo(int nextIndex)
    {
        CheckInitialized();
        
        if (nextIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс записи в логе не может быть отрицательным");
        }

        if (nextIndex > _index!.Count)
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
        CheckInitialized();
        
        if (_index!.Count == 0)
        {
            return LogEntryInfo.Tomb;
        }

        return new LogEntryInfo(_index[^1].Term, _index.Count - 1);
    }
    
    public IReadOnlyList<LogEntry> ReadAll()
    {
        CheckInitialized();
        
        return ReadLogCore(DataStartPosition);
    }

    public IReadOnlyList<LogEntry> ReadFrom(int startIndex)
    {
        CheckInitialized();
        if (_index!.Count == startIndex)
        {
            return Array.Empty<LogEntry>();
        }
        var position = _index[startIndex].Position;
        return ReadLogCore(position);
    }

    private IReadOnlyList<LogEntry> ReadLogCore(long position)
    {
        _file.Seek(position, SeekOrigin.Begin);
        var list = new List<LogEntry>();
        
        while (_reader.PeekChar() != -1)
        {
            var term = new Term(_reader.ReadInt32());
            var data = _reader.ReadString();
            list.Add(new LogEntry(term, data));
        }

        return list;
    }

    public LogEntryInfo GetAt(int index)
    {
        CheckInitialized();
        
        return new LogEntryInfo(_index![index].Term, index);
    }

    private static void Serialize(LogEntry entry, BinaryWriter writer)
    {
        writer.Write(entry.Term.Value);
        writer.Write(entry.Data);
    }

    /// <summary>
    /// Создать новый <see cref="FileLogStorage"/> и тут же его инициализировать
    /// </summary>
    /// <param name="stream">Переданный поток. В проде - файл (<see cref="FileStream"/>)</param>
    /// <returns>Новый, иницилизированный <see cref="FileLogStorage"/></returns>
    /// <exception cref="ArgumentException"><paramref name="stream"/> - не поддерживает чтение, запись или позиционирование</exception>
    /// <exception cref="InvalidDataException">
    /// Обнаружены ошибки во время инициализации файла (потока) данных: <br/>
    ///    - Поток не пуст и при этом его размер меньше минимального (размер заголовка) <br/> 
    ///    - Полученное магическое число не соответствует требуемому <br/>
    ///    - Указанная в файле версия несовместима с текущей <br/>\
    /// </exception>
    /// <exception cref="IOException">Ошибка во время чтения из потока</exception>
    public static FileLogStorage Initialize(Stream stream)
    {
        var logStorage = new FileLogStorage(stream);
        logStorage.Initialize();
        return logStorage;
    }
}