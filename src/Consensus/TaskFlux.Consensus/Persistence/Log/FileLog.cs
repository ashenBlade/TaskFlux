using System.Diagnostics;
using System.IO.Abstractions;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;
using IOException = System.IO.IOException;

namespace TaskFlux.Consensus.Persistence.Log;

public class FileLog : IDisposable
{
    /// <summary>
    /// Маркер файла лога.
    /// Находится в самом начале файла
    /// </summary>
    private const uint FileMarker = 0x1276AD55;

    /// <summary>
    /// Маркер начала записи.
    /// Используется, чтобы разделять записи лога между друг другом
    /// </summary>
    private const uint RecordMarker = 0xAAF534C4;

    /// <summary>
    /// Специальный маркер конца записи, показывающий, что дальше записей нет
    /// </summary>
    private const int EndOfDataRecordMarker = 0;

    /// <summary>
    /// Версия-константа для бинарной совместимости.
    /// Вряд-ли будет использоваться, но звучит значимо
    /// </summary>
    private const int CurrentVersion = 1;

    /// <summary>
    /// Общий размер заголовка в байтах: Маркер + Версия
    /// </summary>
    private const int HeaderSizeBytes = sizeof(int)
                                      + sizeof(int);

    private const long DataStartPosition = HeaderSizeBytes;

    /// <summary>
    /// Файл лога команд
    /// </summary>
    private readonly IFileInfo _logFile;

    /// <summary>
    /// Поток, представляющий файл
    /// </summary>
    private FileSystemStream _logFileStream;

    private record struct LogRecord(Term Term, uint CheckSum, int PayloadLength, long Position)
    {
        public long GetNextRecordPosition()
        {
            return Position       // Начало в файле
                 + sizeof(int)    // Маркер
                 + sizeof(long)   // Терм
                 + sizeof(uint)   // Чек-сумма
                 + sizeof(int)    // Длина данных
                 + PayloadLength; // Размер данных
        }
    }

    /// <summary>
    /// Список отображений: индекс записи - позиция в файле (потоке)
    /// </summary>
    private readonly List<LogRecord> _log;

    /// <summary>
    /// Позиция, начиная с которой необходимо записывать в лог новые элементы.
    /// Если лог пустой, то указывает на <see cref="DataStartPosition"/>,
    /// в противном случае - на конец последней записи, прямо на начало <see cref="EndOfDataRecordMarker"/>.
    /// Т.е. при дозаписи необходимо позиционироваться на эту позицию.
    /// </summary>
    private long GetAppendPosition() =>
        _log.Count == 0
            ? DataStartPosition
            : _log[^1].GetNextRecordPosition();

    /// <summary>
    /// Количество записей в логе
    /// </summary>
    public int Count => _log.Count;

    /// <summary>
    /// Индекс последней записи лога.
    /// Может быть Tomb, если лог пуст
    /// </summary>
    public Lsn LastIndex => _log.Count == 0
                                ? Lsn.Tomb
                                : _log.Count - 1;


    /// <summary>
    /// Индекс, с которого начинаются все записи в логе.
    /// Записи с этим индексом может и не существовать
    /// </summary>
    public Lsn StartIndex => 0;

    private FileLog(FileSystemStream fileStream,
                    IFileInfo logFile,
                    List<LogRecord> log)
    {
        _logFile = logFile;
        _logFileStream = fileStream;
        _log = log;
    }

    public static FileLog Initialize(IDirectoryInfo dataDirectory)
    {
        var logFile =
            dataDirectory.FileSystem.FileInfo.New(Path.Combine(dataDirectory.FullName, Constants.LogFileName));
        var tempDir = dataDirectory.FileSystem.DirectoryInfo.New(Path.Combine(dataDirectory.FullName,
            Constants.TemporaryDirectoryName));

        if (!tempDir.Exists)
        {
            try
            {
                tempDir.Create();
            }
            catch (Exception e)
            {
                throw new IOException("Ошибка во время создания директории для временных файлов", e);
            }
        }

        FileSystemStream fileStream;
        try
        {
            fileStream = logFile.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (Exception e)
        {
            throw new IOException("Ошибка во время создания файла лога", e);
        }

        try
        {
            var index = Initialize(fileStream);
            return new FileLog(fileStream, logFile, index);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    /// <summary>
    /// Прочитать и инициализировать индекс с диска.
    /// Выполняется во время создания объекта
    /// </summary>
    private static List<LogRecord> Initialize(FileSystemStream file)
    {
        try
        {
            var reader = new StreamBinaryReader(file);

            if (file.Length == 0)
            {
                // Файл был пуст
                WriteHeader(file);
                new StreamBinaryWriter(file).Write(EndOfDataRecordMarker);
                file.Flush(true);

                return new List<LogRecord>();
            }

            if (file.Length < HeaderSizeBytes)
            {
                throw new InvalidDataException(
                    $"Минимальный размер файла лога {HeaderSizeBytes} байт. Переданный размер ({file.Length}) меньше минимальной длины");
            }

            file.Seek(0, SeekOrigin.Begin);

            // Валидация заголовка файла

            var fileMarker = reader.ReadUInt32();
            if (fileMarker != FileMarker)
            {
                throw new InvalidDataException(
                    $"Считанный из файла маркер не равен требуемому. Ожидалось: {FileMarker}. Получено: {fileMarker}");
            }

            var version = reader.ReadInt32();
            if (CurrentVersion < version)
            {
                throw new InvalidDataException(
                    $"Указанная версия файла больше текущей версии программы. Текущая версия: {CurrentVersion}. Указанная версия: {version}");
            }

            var index = new List<LogRecord>();

            // Воссоздаем индекс

            try
            {
                long filePosition;
                // ! Не выносить в цикл, т.к. этот спан будет создаваться новый на каждую итерацию
                Span<byte> buffer = stackalloc byte[Environment.SystemPageSize];
                while (( filePosition = file.Position ) < file.Length)
                {
                    // | Маркер | Терм | Чек-сумма | Данные |
                    var recordMarker = reader.ReadUInt32();
                    if (recordMarker != RecordMarker)
                    {
                        if (recordMarker == EndOfDataRecordMarker)
                        {
                            break;
                        }

                        throw new InvalidDataException(
                            $"Из файла прочитан невалидный маркер записи: {recordMarker}. Индекс записи: {index.Count}. Позиция в файле: {file.Position}");
                    }

                    var term = reader.ReadTerm();
                    var storedCheckSum = reader.ReadUInt32();
                    var computedCheckSum = Crc32CheckSum.InitialValue;

                    // Проверяем корректность данных
                    var dataLength = reader.ReadInt32();

                    // Рассчитываем чек-сумму по переданным данным
                    if (dataLength > 0)
                    {
                        var left = dataLength;
                        do
                        {
                            // Используемый для чтения очередного чанка буфер
                            var span = buffer[..Math.Min(left, buffer.Length)];
                            try
                            {
                                file.ReadExactly(span);
                            }
                            catch (EndOfStreamException eose)
                            {
                                throw new InvalidDataException(
                                    "Не удалось прочитать данные из файла для проверки чек-суммы: достигнут конец файла",
                                    eose);
                            }

                            computedCheckSum = Crc32CheckSum.Compute(computedCheckSum, span);
                            left -= span.Length;
                        } while (left > 0);

                        if (storedCheckSum != computedCheckSum)
                        {
                            throw new InvalidDataException(
                                $"Ошибка при валидации чек-суммы: прочитанное значение не равно вычисленному. Прочитано: {storedCheckSum}. Вычислено: {computedCheckSum}. Позиция в файле: {file.Position}");
                        }
                    }

                    index.Add(new LogRecord(term, storedCheckSum, dataLength, filePosition));
                }
            }
            catch (EndOfStreamException e)
            {
                throw new InvalidDataException(
                    "Ошибка при воссоздании индекса из файла лога. Не удалось прочитать указанное количество данных",
                    e);
            }

            return index;
        }
        catch (Exception)
        {
            file.Close();
            throw;
        }
    }

    private static void WriteHeader(Stream stream)
    {
        var writer = new StreamBinaryWriter(stream);
        stream.Seek(0, SeekOrigin.Begin);
        writer.Write(FileMarker);
        writer.Write(CurrentVersion);
    }

    public ulong FileSize => ( ulong ) _logFileStream.Length;

    /// <summary>
    /// Запись новую запись в конец файла лога без коммита
    /// </summary>
    /// <param name="entry">Запись, которую нужно добавить</param>
    public Lsn Append(LogEntry entry)
    {
        var savedAppendPosition = _logFileStream.Seek(GetAppendPosition(), SeekOrigin.Begin);
        var writer = new StreamBinaryWriter(_logFileStream);

        AppendRecordCore(entry, ref writer);

        writer.Write(EndOfDataRecordMarker);

        _logFileStream.Flush(true);

        _log.Add(new LogRecord(entry.Term, entry.GetCheckSum(), entry.Data.Length, savedAppendPosition));

        return _log.Count - 1;
    }

    private static void AppendRecordCore(LogEntry entry, ref StreamBinaryWriter writer)
    {
        writer.Write(RecordMarker);
        writer.Write(entry.Term);
        writer.Write(entry.GetCheckSum());
        writer.WriteBuffer(entry.Data);
    }

    /// <summary>
    /// Записать в конец файла несколько записей без коммита
    /// </summary>
    /// <param name="entries">Записи, которые необходимо записать</param>
    public void AppendRange(IReadOnlyList<LogEntry> entries)
    {
        if (entries.Count == 0)
        {
            return;
        }

        var writer = new StreamBinaryWriter(_logFileStream);
        var startPosition = GetAppendPosition();

        _logFileStream.Seek(startPosition, SeekOrigin.Begin);

        foreach (var entry in entries)
        {
            _log.Add(new LogRecord(entry.Term, entry.GetCheckSum(), entry.Data.Length, _logFileStream.Position));
            AppendRecordCore(entry, ref writer);
        }

        writer.Write(EndOfDataRecordMarker);
        _logFileStream.Flush(true);
    }

    public LogEntryInfo GetPrecedingLogEntryInfo(int nextIndex)
    {
        if (nextIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс записи в логе не может быть отрицательным");
        }

        if (nextIndex > _log.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс записи в логе не может быть больше размера лога");
        }

        return nextIndex == 0
                   ? LogEntryInfo.Tomb
                   : new LogEntryInfo(_log[nextIndex - 1].Term, nextIndex - 1);
    }

    /// <summary>
    /// Получить информацию о последней записи в логе.
    /// Индекс указывает на индекс записи в файле - не глобальный
    /// </summary>
    /// <returns>Последняя запись в логе. Может быть <see cref="LogEntryInfo.Tomb"/>, если записей нет</returns>
    public LogEntryInfo GetLastLogEntry()
    {
        if (_log.Count == 0)
        {
            return LogEntryInfo.Tomb;
        }

        return new LogEntryInfo(_log[^1].Term, _log.Count - 1);
    }

    public LogEntryInfo GetInfoAt(Lsn index)
    {
        return new LogEntryInfo(_log[( int ) index].Term, index);
    }

    public void Dispose()
    {
        _logFileStream.Flush(true);
        _logFileStream.Close();
        _logFileStream.Dispose();
        _log.Clear();
    }

    /// <summary>
    /// Метод для чтения всех записей из файла снапшота.
    /// </summary>
    /// <remarks>Используется для тестов</remarks>
    internal IReadOnlyList<LogEntry> ReadAllTest()
    {
        return ReadRangeCore(0, _log.Count);
    }

    /// <summary>
    /// Прочитать записи с указанного индекса включительно.
    /// </summary>
    /// <param name="lsn">Индекс (локальный) в логе</param>
    /// <returns>Список из прочитанных записей</returns>
    public IReadOnlyList<LogEntry> GetFrom(Lsn lsn)
    {
        if (_log.Count < lsn)
        {
            throw new ArgumentOutOfRangeException(nameof(lsn), lsn,
                $"Указанный индекс больше количества записей в логе: {_log.Count}");
        }

        if (lsn == _log.Count)
        {
            return Array.Empty<LogEntry>();
        }

        return ReadRangeCore(lsn, _log.Count - ( int ) lsn);
    }

    /// <summary>
    /// Прочитать записи из указанного диапазона индексов
    /// </summary>
    /// <param name="start">Индекс первой записи</param>
    /// <param name="count">Количество записей, которые нужно прочитать</param>
    /// <returns>Список из прочитанных записей</returns>
    private IReadOnlyList<LogEntry> ReadRangeCore(Lsn start, int count)
    {
        Debug.Assert(start + count <= _log.Count, "start + count < _index.Count",
            "Индекс начала чтения не может быть больше индекса окончания. Индекс начала: {0}. Количество записей: {1}",
            start, count);
        if (_log.Count == 0)
        {
            return Array.Empty<LogEntry>();
        }

        var startPosition = _log[( int ) start].Position;
        _logFileStream.Seek(startPosition, SeekOrigin.Begin);

        var i = 1;
        var reader = new StreamBinaryReader(_logFileStream);
        var entries = new List<LogEntry>(count);

        while (TryReadLogEntry(ref reader, out var entry, checkCrc: true))
        {
            entries.Add(entry);
            if (count < ++i)
            {
                break;
            }
        }

        return entries;
    }

    /// <summary>
    /// Записать указанные записи в файл с перезаписью незакоммиченных записей.
    /// </summary>
    /// <param name="entries">Записи, которые необходимо записать</param>
    /// <param name="index">Индекс, начиная с которого необходимо записывать данные</param>
    public void InsertRangeOverwrite(IReadOnlyList<LogEntry> entries, Lsn index)
    {
        if (entries.Count == 0)
        {
            return;
        }

        if (index < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index,
                "Индекс записи не может быть отрицательным");
        }

        /*
         * Тут могут быть 2 случая:
         * 1. Записать в конец файла - localIndex = _index.Count
         * 2. Перезаписать существующие записи - localIndex < _index.Count
         */

        if (index == _log.Count)
        {
            AppendRange(entries);
            return;
        }

        if (_log.Count < index)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index,
                $"Индекс записи превышает размер лога. Текущий размер лога: {_log.Count}");
        }

        // Записываем данные в лог
        var startPosition = _log[( int ) index].Position;
        _logFileStream.Position = startPosition;
        var writer = new StreamBinaryWriter(_logFileStream);
        var newIndexValues = new List<LogRecord>();

        var hasData = false;

        foreach (var entry in entries)
        {
            newIndexValues.Add(new LogRecord(entry.Term, entry.GetCheckSum(), entry.Data.Length,
                _logFileStream.Position));
            AppendRecordCore(entry, ref writer);
            hasData = true;
        }

        if (hasData)
        {
            writer.Write(EndOfDataRecordMarker);
            _logFileStream.Flush(true);


            // Обновляем индекс
            _log.RemoveRange(( int ) index, ( int ) ( _log.Count - index ));
            _log.AddRange(newIndexValues);
        }
    }

    private static bool TryReadLogEntry(ref StreamBinaryReader reader, out LogEntry entry, bool checkCrc = false)
    {
        var recordMarker = reader.ReadUInt32();
        if (recordMarker != RecordMarker)
        {
            Debug.Assert(recordMarker == EndOfDataRecordMarker, "recordMarker == EndOfDataRecordMarker",
                "Прочитанный маркер не равняется маркеру начала или конца записи");
            entry = default!;
            return false;
        }

        var term = reader.ReadTerm();
        var checkSum = reader.ReadUInt32();
        var buffer = reader.ReadBuffer();
        if (checkCrc)
        {
            var computedCheckSum = Crc32CheckSum.Compute(buffer);
            if (checkSum != computedCheckSum)
            {
                throw new InvalidDataException(
                    $"Прочитанная чек-сумма не равна вычисленной. Позиция записи в файле: {reader.Stream.Position - buffer.Length - sizeof(int) - sizeof(int) - sizeof(int)}. Прочитанная чек-сумма: {checkSum}. Вычисленная чек-сумма: {computedCheckSum}");
            }
        }

        entry = new LogEntry(term, buffer);
        return true;
    }

    internal void SetupLogTest(IReadOnlyList<LogEntry> entries)
    {
        var writer = new StreamBinaryWriter(_logFileStream);

        _logFileStream.Seek(DataStartPosition, SeekOrigin.Begin);

        foreach (var entry in entries)
        {
            _log.Add(new LogRecord(entry.Term, entry.GetCheckSum(), entry.Data.Length, _logFileStream.Position));
            AppendRecordCore(entry, ref writer);
        }

        writer.Write(EndOfDataRecordMarker);
        _logFileStream.Flush(true);
    }

    /// <summary>
    /// Прочитать из файла указанный диапазон данных.
    /// Границы диапазона (<paramref name="start"/>, <paramref name="end"/>) включаются
    /// </summary>
    /// <returns>Перечисление всех данных из указанного диапазона</returns>
    /// <remarks>Перечисление ленивое</remarks>
    public IEnumerable<byte[]> ReadDataRange(Lsn start, Lsn end)
    {
        if (end < start)
        {
            throw new ArgumentOutOfRangeException(nameof(start), start,
                $"Индекс начала не может быть больше индекса окончания. Индекс конца: {end}");
        }

        if (_log.Count <= start)
        {
            throw new ArgumentOutOfRangeException(nameof(start), start,
                $"Указанный индекс начала чтения больше количества записей в логе. Размер лога: {_log.Count}");
        }

        if (_log.Count <= end)
        {
            throw new ArgumentOutOfRangeException(nameof(end), end,
                $"Указанный индекс конца чтения больше количества записей в логе. Размер лога: {_log.Count}");
        }

        var count = ( long ) ( end - start ) + 1;
        var i = 1;

        var startPosition = _log[( int ) start].Position;
        _logFileStream.Seek(startPosition, SeekOrigin.Begin);
        var reader = new StreamBinaryReader(_logFileStream);
        while (TryReadLogEntry(ref reader, out var entry, checkCrc: false))
        {
            yield return entry.Data;
            if (count < ++i)
            {
                yield break;
            }
        }
    }

    /// <summary>
    /// Прочитать закоммиченные записи из лога
    /// </summary>
    /// <returns>Список из закоммиченных записей</returns>
    internal IReadOnlyList<LogEntry> GetAllEntriesTest()
    {
        return ReadRangeCore(0, _log.Count);
    }

    public bool TryGetLogEntryInfo(Lsn index, out LogEntryInfo entry)
    {
        if (_log.Count <= index)
        {
            entry = LogEntryInfo.Tomb;
            return false;
        }

        if (index.IsTomb)
        {
            entry = LogEntryInfo.Tomb;
            return false;
        }

        entry = new LogEntryInfo(_log[( int ) index].Term, index);
        return true;
    }

    /// <summary>
    /// Очистить лог полностью и обновить индекс коммита
    /// </summary>
    public void Clear()
    {
        if (_log.Count == 0)
        {
            return;
        }

        // Обновляем индекс коммита
        _logFileStream.Seek(DataStartPosition, SeekOrigin.Begin);
        var writer = new StreamBinaryWriter(_logFileStream);
        // Очищаем содержимое файла лога
        writer.Write(EndOfDataRecordMarker);
        _logFileStream.Flush(true);
        _log.Clear();
    }

    internal void ValidateFileTest()
    {
        _ = Initialize(_logFileStream);
    }
}