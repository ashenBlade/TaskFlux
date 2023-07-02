using System.Runtime.CompilerServices;
using System.Text;
using Raft.Core;
using Raft.Core.Log;

namespace Raft.Storage.File;

public class FileLogStorage: ILogStorage
{
    private const int Marker = 0x2F6F0F2F;

    /// <summary>
    /// Позиция, на которой расположено число версии
    /// </summary>
    private const int VersionPositionByte = 4;

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

    /// <summary>
    /// Список отображений: индекс записи - позиция в файле (потоке)
    /// </summary>
    private List<PositionTerm>? _index;
    private List<PositionTerm> Index => _index ?? throw new IncompleteInitializationException("Список индексов не был инициализирован");
    
    /// <summary>
    /// Флаг инициализации
    /// </summary>
    private bool _initialized;
    
    public FileLogStorage(Stream file)
    {
        if (!file.CanRead)
        {
            throw new IOException("Переданный поток не поддерживает чтение");
        }

        if (!file.CanSeek)
        {
            throw new IOException("Переданный поток не поддерживает позиционирование");
        }

        if (!file.CanWrite)
        {
            throw new IOException("Переданный поток не поддерживает запись");
        }
        
        _file = file;
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
            // Файл пуст
            using (var writer = new BinaryWriter(_file, Encoding, true))
            {
                writer.Write(Marker);
                writer.Write(CurrentVersion);
            }
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
        using var reader = new BinaryReader(_file, Encoding, true);
        var marker = reader.ReadInt32();
        if (marker != Marker)
        {
            throw new InvalidDataException($"Считанный из файла маркер не равен требуемому. Ожидалось: {Marker}. Получено: {marker}");
        }

        var version = reader.ReadInt32();
        if (CurrentVersion < version)
        {
            throw new InvalidDataException(
                $"Указанная версия файла меньше текущей версии программы. Текущая версия: {CurrentVersion}. Указанная версия: {version}");
        }

        var index = new List<PositionTerm>();
        
        // Воссоздаем индекс
        try
        {
            while (reader.PeekChar() != -1)
            {
                var position = _file.Position;
                var term = reader.ReadInt32();
                var stringLength = reader.Read7BitEncodedInt();
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
        
        var initialPosition = _file.Position;
        try
        {
            using var writer = new BinaryWriter(_file, Encoding, true);
            Serialize(entry, writer);
            writer.Flush();
        }
        catch (Exception)
        {
            try { _file.Position = initialPosition; } catch (Exception) { /* */ }
            throw;
        }
        
        Index.Add(new PositionTerm(entry.Term, initialPosition));
        return new LogEntryInfo(entry.Term, Index.Count - 1);
    }

    public LogEntryInfo AppendRange(IEnumerable<LogEntry> entries, int index)
    {
        CheckInitialized();
        
        if (Index.Count < index)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index, $"Индекс для вставки записи превысил наибольший хранимый индекс {Index.Count}");
        }

        
        // Вместо поочередной записи используем буффер в памяти.
        // Сначала запишем сериализованные данные на него, одновременно создавая новые записи индекса.
        // После быстро запишем данные на диск и обновим список индексов 

        var entriesArray = entries.ToArray();
        
        var newIndexes = new List<PositionTerm>();
        using var memory = new MemoryStream(( sizeof(int) + 128 ) * entriesArray.Length);
        using var writer = new BinaryWriter(memory, Encoding, true);
        var startPosition = _file.Position + 1;
        foreach (var entry in entriesArray)
        {
            writer.Write(entry.Term.Value);
            writer.Write(entry.Data);
            newIndexes.Add(new PositionTerm(entry.Term, startPosition + memory.Length));
        }
        
        var dataBytes = memory.ToArray();
        
        _file.Seek(0, SeekOrigin.End);
        
        if (index == Index.Count)
        {
            _file.Seek(0, SeekOrigin.End);
        }
        else
        {
            _file.Position = Index[index].Position;
        }
        
        _file.Write(dataBytes);
        _file.Flush();
            
        _index!.RemoveRange(index, _index.Count - index);
        _index.AddRange(newIndexes);

        return GetLastLogEntryInfoCore();
    }

    private LogEntryInfo GetLastLogEntryInfoCore()
    {
        CheckInitialized();
        
        return Index.Count == 0
                   ? LogEntryInfo.Tomb
                   : new LogEntryInfo(Index[^1].Term, Index.Count - 1);
    }
    

    public LogEntryInfo GetPrecedingLogEntryInfo(int nextIndex)
    {
        CheckInitialized();
        
        if (nextIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс записи в логе не может быть отрицательным");
        }

        if (nextIndex > Index.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс записи в логе не может быть больше размера лога");
        }

        return nextIndex == 0
                   ? LogEntryInfo.Tomb
                   : new LogEntryInfo(Index[nextIndex - 1].Term, nextIndex - 1);
    }

    public LogEntryInfo GetLastLogEntry()
    {
        CheckInitialized();
        
        if (Index.Count == 0)
        {
            return LogEntryInfo.Tomb;
        }

        return new LogEntryInfo(Index[^1].Term, Index.Count - 1);
    }
    
    public IReadOnlyList<LogEntry> ReadAll()
    {
        CheckInitialized();
        
        return ReadLogCore(DataStartPosition);
    }

    public IReadOnlyList<LogEntry> ReadFrom(int startIndex)
    {
        CheckInitialized();
        if (Index.Count == startIndex)
        {
            return Array.Empty<LogEntry>();
        }
        var position = Index[startIndex].Position;
        return ReadLogCore(position);
    }

    private IReadOnlyList<LogEntry> ReadLogCore(long position)
    {
        _file.Seek(position, SeekOrigin.Begin);
        var list = new List<LogEntry>();
        using var reader = new BinaryReader(_file, Encoding, true);
        while (reader.PeekChar() != -1)
        {
            var term = new Term(reader.ReadInt32());
            var data = reader.ReadString();
            list.Add(new LogEntry(term, data));
        }

        return list;
    }

    public LogEntryInfo GetAt(int index)
    {
        CheckInitialized();
        
        return new LogEntryInfo(Index[index].Term, index);
    }

    private static void Serialize(LogEntry entry, BinaryWriter writer)
    {
        writer.Write(entry.Term.Value);
        writer.Write(entry.Data);
    }

    public static FileLogStorage Initialize(Stream fileStream)
    {
        var logStorage = new FileLogStorage(fileStream);
        logStorage.Initialize();
        return logStorage;
    }
}