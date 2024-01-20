using System.Diagnostics;
using System.IO.Abstractions;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;

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
    /// Общий размер заголовка в байтах: Маркер + Версия + Индекс коммита
    /// </summary>
    private const int HeaderSizeBytes = sizeof(int)
                                      + sizeof(int)
                                      + sizeof(int);

    /// <summary>
    /// Индекс в файле, где располагается число закоммиченных записей
    /// </summary>
    private const int CommitIndexPosition = sizeof(int) + sizeof(int);

    private const long DataStartPosition = HeaderSizeBytes;

    /// <summary>
    /// Файл лога команд
    /// </summary>
    private readonly IFileInfo _logFile;

    /// <summary>
    /// Директория для временных файлов
    /// </summary>
    private readonly IDirectoryInfo _temporaryDirectory;

    /// <summary>
    /// Поток, представляющий файл
    /// </summary>
    private FileSystemStream _logFileStream = null!;

    /// <summary>
    /// Список отображений: индекс записи - позиция в файле (потоке)
    /// </summary>
    private List<PositionTerm> _index = null!;

    /// <summary>
    /// Позиция, начиная с которой необходимо записывать в лог новые элементы.
    /// Если лог пустой, то указывает на <see cref="DataStartPosition"/>,
    /// в противном случае - на конец последней записи, прямо на начало <see cref="EndOfDataRecordMarker"/>.
    /// Т.е. при дозаписи необходимо позиционироваться на эту позицию.
    /// </summary>
    /// <remarks>
    /// Необходимо постоянно отслеживать и обновлять этот указатель
    /// </remarks>
    private long _appendPosition = DataStartPosition;

    /// <summary>
    /// Получить позицию в файле, по которому необходимо производить дозапись новых элементов
    /// </summary>
    /// <returns></returns>
    private long GetAppendPosition()
    {
        return _appendPosition;
    }

    /// <summary>
    /// Индекс последней закоммиченной записи.
    /// Содержит индекс записи из лога, а не глобальной закоммиченной записи.
    /// </summary>
    public int CommitIndex { get; private set; }

    /// <summary>
    /// Количество записей в логе с учетом закоммиченных и нет.
    /// </summary>
    public int Count => _index.Count;

    /// <summary>
    /// Получить индекс последней закоммиченной записи.
    /// Если закоммиченных записей нет, то возвращается <c>false</c>
    /// </summary>
    /// <param name="commitIndex">Хранившийся индекс закоммиченной записи</param>
    /// <returns><c>true</c> - есть закоммченные записи, <c>false</c> - иначе</returns>
    public bool TryGetCommitIndex(out int commitIndex)
    {
        if (CommitIndex == NoCommittedEntriesIndex)
        {
            commitIndex = 0;
            return false;
        }

        commitIndex = CommitIndex;
        return true;
    }

    /// <summary>
    /// Значение коммита, указывающее, что никакая запись еще не закоммичена
    /// </summary>
    private const int NoCommittedEntriesIndex = -1;

    internal FileLog(IFileInfo logFile, IDirectoryInfo temporaryDirectory)
    {
        _logFile = logFile;
        _temporaryDirectory = temporaryDirectory;

        InitializeCtor();
    }

    private void InitializeCtor()
    {
        // Разделяю, чтобы выводить более информативные сообщения об ошибках
        if (!_logFile.Exists)
        {
            try
            {
                _logFileStream = _logFile.Create();
            }
            catch (UnauthorizedAccessException uae)
            {
                throw new IOException("Не удалось создать новый файл лога команд: нет прав для создания файла", uae);
            }
            catch (IOException e)
            {
                throw new IOException("Не удалось создать новый файл лога команд", e);
            }
        }
        else
        {
            try
            {
                _logFileStream = _logFile.Open(FileMode.Open);
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

        ( _index, CommitIndex, _appendPosition ) = Initialize(_logFileStream);
    }

    /// <summary>
    /// Прочитать и инициализировать индекс с диска.
    /// Выполняется во время создания объекта
    /// </summary>
    private static (List<PositionTerm> Index, int CommitIndex, long AppendPosition) Initialize(FileSystemStream file)
    {
        try
        {
            var reader = new StreamBinaryReader(file);

            if (file.Length == 0)
            {
                // Файл был пуст
                WriteHeader(file, NoCommittedEntriesIndex);
                new StreamBinaryWriter(file).Write(EndOfDataRecordMarker);
                file.Flush(true);

                return ( new List<PositionTerm>(), NoCommittedEntriesIndex, DataStartPosition );
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

            var commitIndex = reader.ReadInt32();
            if (commitIndex is < 0 and not NoCommittedEntriesIndex)
            {
                throw new InvalidDataException(
                    $"Индекс закоммиченной записи меньше 0 и при этом не равен -1. Прочитанный индекс: {commitIndex}");
            }

            var index = new List<PositionTerm>();
            var appendPosition = DataStartPosition;

            // Воссоздаем индекс

            try
            {
                long filePosition;
                // 4 Кб - размер 1 страницы
                const int pageSize = 4096;
                // ! Не выносить в цикл, т.к. этот спан будет создаваться новый на каждую итерацию
                Span<byte> buffer = stackalloc byte[pageSize];
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

                    var term = reader.ReadInt32();
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

                    index.Add(new PositionTerm(term, filePosition));
                    appendPosition = file.Position;
                }
            }
            catch (EndOfStreamException e)
            {
                throw new InvalidDataException(
                    "Ошибка при воссоздании индекса из файла лога. Не удалось прочитать указанное количество данных",
                    e);
            }

            if (index.Count <= commitIndex)
            {
                throw new InvalidDataException(
                    $"Индекс закоммиченной записи больше количества записей в логе. Индекс коммита: {commitIndex}. Записей в логе: {index.Count}");
            }

            return ( index, commitIndex, appendPosition );
        }
        catch (Exception)
        {
            file.Close();
            throw;
        }
    }

    private static void WriteHeader(Stream stream, int commitIndex)
    {
        var writer = new StreamBinaryWriter(stream);
        stream.Seek(0, SeekOrigin.Begin);
        writer.Write(FileMarker);
        writer.Write(CurrentVersion);
        writer.Write(commitIndex);
    }

    // Очень надеюсь, что такого никогда не произойдет
    // Чтобы такого точно не произошло, надо на уровне выставления максимального размера файла лога ограничение сделать
    public ulong FileSize => checked( ( ulong ) _logFileStream.Length );

    /// <summary>
    /// Запись новую запись в конец файла лога без коммита
    /// </summary>
    /// <param name="entry">Запись, которую нужно добавить</param>
    public LogEntryInfo Append(LogEntry entry)
    {
        var savedAppendPosition = _logFileStream.Seek(_appendPosition, SeekOrigin.Begin);
        var writer = new StreamBinaryWriter(_logFileStream);

        AppendRecordCore(entry, ref writer);

        _appendPosition = _logFileStream.Position;
        writer.Write(EndOfDataRecordMarker);

        _logFileStream.Flush(true);

        _index.Add(new PositionTerm(entry.Term, savedAppendPosition));

        return new LogEntryInfo(entry.Term, _index.Count - 1);
    }

    private static void AppendRecordCore(LogEntry entry, ref StreamBinaryWriter writer)
    {
        writer.Write(RecordMarker);
        writer.Write(entry.Term.Value);
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
        var startPosition = _index.Count == 0
                                ? DataStartPosition
                                : _appendPosition;
        _logFileStream.Seek(startPosition, SeekOrigin.Begin);

        foreach (var entry in entries)
        {
            _index.Add(new PositionTerm(entry.Term, _logFileStream.Position));
            AppendRecordCore(entry, ref writer);
        }

        _appendPosition = _logFileStream.Position;
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

        if (nextIndex > _index.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(nextIndex), nextIndex,
                "Следующий индекс записи в логе не может быть больше размера лога");
        }

        return nextIndex == 0
                   ? LogEntryInfo.Tomb
                   : new LogEntryInfo(_index[nextIndex - 1].Term, nextIndex - 1);
    }

    /// <summary>
    /// Получить информацию о последней записи в логе.
    /// Индекс указывает на индекс записи в файле - не глобальный
    /// </summary>
    /// <returns>Последняя запись в логе. Может быть <see cref="LogEntryInfo.Tomb"/>, если записей нет</returns>
    public LogEntryInfo GetLastLogEntry()
    {
        if (_index.Count == 0)
        {
            return LogEntryInfo.Tomb;
        }

        return new LogEntryInfo(_index[^1].Term, _index.Count - 1);
    }

    /// <summary>
    /// Получить данные о последней записи в логе, если она есть
    /// </summary>
    /// <param name="lastLogEntry">Последняя запись в логе</param>
    /// <returns><c>true</c> - в логе были записи, <c>false</c> - лог пуст</returns>
    public bool TryGetLastLogEntry(out LogEntryInfo lastLogEntry)
    {
        if (_index.Count == 0)
        {
            lastLogEntry = LogEntryInfo.Tomb;
            return false;
        }

        lastLogEntry = new LogEntryInfo(_index[^1].Term, _index.Count - 1);
        return true;
    }

    public IReadOnlyList<LogEntry> ReadFrom(int startIndex)
    {
        if (_index.Count <= startIndex)
        {
            return Array.Empty<LogEntry>();
        }

        var position = _index[startIndex].Position;
        return ReadLogCoreTest(position);
    }

    /// <summary>
    /// Читать данные из файла постепенно, начиная с указанной позиции
    /// </summary>
    /// <param name="position">Позиция в файле, с которой нужно читать записи. Указывает на начало</param>
    /// <returns>Поток записей лога</returns>
    /// <exception cref="InvalidDataException">В файле представлены неверные данные</exception>
    /// <remarks>
    /// Метод будет читать до конца, пока не дойдет до конца файла,
    /// поэтому вызывающий должен контролировать сколько записей он хочет получить
    /// </remarks>
    private List<LogEntry> ReadLogCoreTest(long position)
    {
        _logFileStream.Seek(position, SeekOrigin.Begin);

        var result = new List<LogEntry>();
        var reader = new StreamBinaryReader(_logFileStream);
        while (_logFileStream.Position < _logFileStream.Length)
        {
            // | Маркер | Терм | Чек-сумма | Данные |
            var recordMarker = reader.ReadUInt32();
            if (recordMarker != RecordMarker)
            {
                Debug.Assert(recordMarker == EndOfDataRecordMarker, "recordMarker == EndOfDataRecordMarker",
                    "Прочитан не маркер записи и не маркер окончания записей");
                break;
            }

            var term = reader.ReadInt32();
            var storedCheckSum = reader.ReadUInt32();
            var payload = reader.ReadBuffer();
            Debug.Assert(Crc32CheckSum.Compute(payload) == storedCheckSum,
                "Crc32CheckSum.Compute(payload) == storedCheckSum", "Чек-суммы не совпадают");
            result.Add(new LogEntry(new Term(term), payload));
        }

        return result;
    }

    public LogEntryInfo GetInfoAt(int index)
    {
        return new LogEntryInfo(_index[index].Term, index);
    }

    /// <summary>
    /// Создать новый <see cref="FileLog"/> и тут же его инициализировать.
    /// Используется во время старта приложения.
    /// </summary>
    /// <param name="dataDirectory">
    /// Информация о директории с данными рафта.
    /// В базовой директории - это `{BASE}/consensus`.
    /// В ней должен лежать файл с логом - `raft.log`
    /// </param>
    /// <param name="temporaryDirectory">
    /// Директория для временных файлов.
    /// Должна существовать на момент вызова функции
    /// </param>
    /// <returns>Новый, инициализированный <see cref="FileLog"/></returns>
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
    /// - Ошибка во время открытия существующего файла лога, либо <br/>
    /// </exception>
    public static FileLog InitializeFromFileSystem(IDirectoryInfo dataDirectory,
                                                   IDirectoryInfo temporaryDirectory)
    {
        var fileStream = OpenOrCreateLogFile();
        if (!temporaryDirectory.Exists)
        {
            throw new ArgumentException(
                $"Директории для временных файлов не существует. Директория: {temporaryDirectory.FullName}");
        }

        return new FileLog(fileStream, temporaryDirectory);

        IFileInfo OpenOrCreateLogFile()
        {
            var file = dataDirectory.FileSystem.FileInfo.New(Path.Combine(dataDirectory.FullName,
                Constants.LogFileName));
            return file;
        }
    }

    public void Dispose()
    {
        _logFileStream.Flush(true);
        _logFileStream.Close();
        _logFileStream.Dispose();
        _index.Clear();
        _appendPosition = DataStartPosition;
    }

    /// <summary>
    /// Метод для чтения всех записей из файла снапшота.
    /// </summary>
    /// <remarks>Используется для тестов</remarks>
    internal IReadOnlyList<LogEntry> ReadAllTest()
    {
        return ReadRangeCore(0, _index.Count);
    }


    /// <summary>
    /// Очистить лог команд до указанного индекса включительно.
    /// После этой операции, из файла лога будут удалены все записи до указанной включительно.
    /// </summary>
    /// <param name="removeUntil">Индекс последней записи, которую нужно удалить</param>
    /// <remarks>Если указанный индекс больше или равен количеству записей в логе, то лог просто очищается</remarks>
    public void TruncateUntil(int removeUntil)
    {
        if (removeUntil < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(removeUntil), removeUntil,
                "Индекс записи в логе не может быть отрицательным");
        }

        if (_index.Count == 0)
        {
            // Очищать ничего не нужно
            return;
        }

        var newCommitIndex = Math.Max(CommitIndex - removeUntil - 1, -1);
        long appendPosition;

        // 1. Создаем временный файл лога
        var (file, stream) = CreateTempLogFile();
        try
        {
            // Инициализируем заголовок
            WriteHeader(stream, newCommitIndex);
            var copyStartPosition = _appendPosition;
            if (removeUntil < _index.Count - 1)
            {
                // Если очистить нужно не весь файл, то
                // Копируем данные с исходного файла лога
                copyStartPosition = _index[removeUntil + 1].Position;
                _logFileStream.Seek(copyStartPosition, SeekOrigin.Begin);
                _logFileStream.CopyTo(stream);
            }

            appendPosition = DataStartPosition + _appendPosition - copyStartPosition;
            // TODO: вот тут надо правильно обновлять индекс последней записи

            // Записываем в конец маркер окончания данных
            var writer = new StreamBinaryWriter(stream);
            // BUG: этот индекс может указывать на конец файла - т.е. EndOfDataMarker и тогда их 2 будет 2
            // appendPosition = stream.Position;

            writer.Write(EndOfDataRecordMarker);

            // Переименовываем в целевой файл
            file.MoveTo(_logFile.FullName, true);
        }
        catch (Exception)
        {
            stream.Close();
            stream.Dispose();
            throw;
        }

        stream.Flush(true);
        // Обновляем свое состояние

        var oldLogFile = _logFileStream;
        _logFileStream = stream;
        CommitIndex = newCommitIndex;
        _appendPosition = appendPosition;
        RemoveIndexPrefix(removeUntil + 1);

        oldLogFile.Dispose();
        return;

        (IFileInfo, FileSystemStream) CreateTempLogFile()
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
                catch (IOException)
                {
                }
            }
        }

        void RemoveIndexPrefix(int count)
        {
            if (_index.Count <= count || count == 0)
            {
                _index.Clear();
                return;
            }

            var positionDelta = _index[count].Position - DataStartPosition;
            _index.RemoveRange(0, count);
            for (var i = 0; i < _index.Count; i++)
            {
                var pt = _index[i];
                _index[i] = pt with {Position = pt.Position - positionDelta};
            }
        }
    }

    /// <summary>
    /// Прочитать записи с указанного индекса включительно.
    /// </summary>
    /// <param name="localIndex">Индекс (локальный) в логе</param>
    /// <returns>Список из прочитанных записей</returns>
    public IReadOnlyList<LogEntry> GetFrom(int localIndex)
    {
        if (_index.Count < localIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(localIndex), localIndex,
                $"Указанный индекс больше количества записей в логе: {_index.Count}");
        }

        if (localIndex == _index.Count)
        {
            return Array.Empty<LogEntry>();
        }

        return ReadRangeCore(localIndex, _index.Count - localIndex);
    }

    /// <summary>
    /// Прочитать записи из указанного диапазона индексов
    /// </summary>
    /// <param name="start">Индекс первой записи</param>
    /// <param name="count">Количество записей, которые нужно прочитать</param>
    /// <returns>Список из прочитанных записей</returns>
    private IReadOnlyList<LogEntry> ReadRangeCore(int start, int count)
    {
        Debug.Assert(start + count <= _index.Count, "start + count < _index.Count",
            "Индекс начала чтения не может быть больше индекса окончания. Индекс начала: {0}. Количество записей: {1}",
            start, count);
        if (_index.Count == 0)
        {
            return Array.Empty<LogEntry>();
        }

        var startPosition = _index[start].Position;
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
    public void InsertRangeOverwrite(IReadOnlyList<LogEntry> entries, int index)
    {
        try
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

            if (_index.Count < index)
            {
                throw new ArgumentOutOfRangeException(nameof(index), index,
                    $"Индекс записи превышает размер лога. Текущий размер лога: {_index.Count}");
            }

            if (index <= CommitIndex)
            {
                throw new ArgumentOutOfRangeException(nameof(index), index,
                    $"Нельзя перезаписать закоммиченные записи. Индекс закоммиченной записи: {CommitIndex}");
            }

            /*
             * Тут могут быть 2 случая:
             * 1. Записать в конец файла - localIndex = _index.Count
             * 2. Перезаписать существующие записи - localIndex < _index.Count
             */

            if (index == _index.Count)
            {
                AppendRange(entries);
                return;
            }

            // Записываем данные в лог
            var startPosition = _index[index].Position;
            _logFileStream.Position = startPosition;
            var writer = new StreamBinaryWriter(_logFileStream);
            var newIndexValues = new List<PositionTerm>();

            var hasData = false;

            foreach (var entry in entries)
            {
                newIndexValues.Add(new PositionTerm(entry.Term, _logFileStream.Position));
                AppendRecordCore(entry, ref writer);
                hasData = true;
            }

            if (hasData)
            {
                _appendPosition = _logFileStream.Position;
                writer.Write(EndOfDataRecordMarker);
                _logFileStream.Flush(true);


                // // Обновляем индекс
                // TODO: возможно в этом месте ошибка
                _index.RemoveRange(index, _index.Count - index);
                _index.AddRange(newIndexValues);
            }
        }
        finally
        {
            // Проверяем целостность файла - ЭТО ТОЛЬКО НА ВРЕМЯ ДЕБАГА!!!
            try
            {
                Initialize(_logFileStream);
            }
            catch (Exception e)
            {
                throw new InvalidDataException($"Ошибка при валидации файла после дозаписи. Запись по индексу: {index}",
                    e);
            }
        }
    }

    /// <summary>
    /// Закоммитить все записи, до указанного индекса включительно
    /// </summary>
    /// <param name="index">Индекс в логе, который нужно закоммитить</param>
    public void Commit(int index)
    {
        if (_index.Count <= index)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index,
                "Индекс для коммита превышает размер лога");
        }

        if (index < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index,
                "Индекс для коммита не может быть отрицательным");
        }

        if (index <= CommitIndex)
        {
            // Не знаю как правильно реагировать на то, что индекс для коммита меньше реально закоммиченных записей. 
            // Пока просто буду завершать работу - все уже сделано
            return;
        }

        _logFileStream.Seek(CommitIndexPosition, SeekOrigin.Begin);
        var writer = new StreamBinaryWriter(_logFileStream);
        writer.Write(index);
        _logFileStream.Flush(true);
        CommitIndex = index;
    }

    internal IReadOnlyList<LogEntry> GetUncommittedTest()
    {
        if (_index.Count == 0)
        {
            // Лог пуст
            return Array.Empty<LogEntry>();
        }

        // Не работает, когда в логе только 1 запись
        if (CommitIndex == _index.Count - 1)
        {
            // Все закоммичены, либо лог пуст
            return Array.Empty<LogEntry>();
        }

        return ReadRangeCore(CommitIndex + 1, _index.Count - CommitIndex - 1);
    }

    private static bool TryReadLogEntry(ref StreamBinaryReader reader, out LogEntry entry, bool checkCrc = false)
    {
        var recordMarker = reader.ReadUInt32();
        if (recordMarker != RecordMarker)
        {
            Debug.Assert(recordMarker == EndOfDataRecordMarker, "recordMarker == EndOfDataRecordMarker",
                "Прочитанный маркер не равняется маркеру начала или конца записи");
            entry = default;
            return false;
        }

        var term = reader.ReadInt32();
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

    internal int ReadCommitIndexTest()
    {
        _logFileStream.Seek(CommitIndexPosition, SeekOrigin.Begin);
        var reader = new StreamBinaryReader(_logFileStream);
        return reader.ReadInt32();
    }

    internal void SetupLogTest(IReadOnlyList<LogEntry> committed, IReadOnlyList<LogEntry> uncommitted)
    {
        var writer = new StreamBinaryWriter(_logFileStream);

        _logFileStream.Seek(CommitIndexPosition, SeekOrigin.Begin);
        CommitIndex = committed.Count - 1;
        writer.Write(CommitIndex);
        _logFileStream.Seek(DataStartPosition, SeekOrigin.Begin);

        foreach (var entry in committed.Concat(uncommitted))
        {
            _index.Add(new PositionTerm(entry.Term, _logFileStream.Position));
            AppendRecordCore(entry, ref writer);
        }

        _appendPosition = _logFileStream.Position;
        writer.Write(EndOfDataRecordMarker);
        _logFileStream.Flush(true);
    }

    /// <summary>
    /// Прочитать из файла закоммиченные данные
    /// </summary>
    /// <returns>Перечисление всех данных из закоммиченных записей лога</returns>
    /// <remarks>Перечисление ленивое</remarks>
    public IEnumerable<byte[]> ReadCommittedData()
    {
        if (_index.Count == 0)
        {
            yield break;
        }

        if (CommitIndex == NoCommittedEntriesIndex)
        {
            yield break;
        }

        var count = CommitIndex + 1;
        var i = 1;
        _logFileStream.Seek(DataStartPosition, SeekOrigin.Begin);
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
    internal IReadOnlyList<LogEntry> GetCommittedTest()
    {
        if (CommitIndex == NoCommittedEntriesIndex)
        {
            return Array.Empty<LogEntry>();
        }

        return ReadRangeCore(0, CommitIndex + 1);
    }

    public bool TryGetLogEntryInfo(int index, out LogEntryInfo entry)
    {
        if (_index.Count <= index)
        {
            entry = LogEntryInfo.Tomb;
            return false;
        }

        entry = new LogEntryInfo(_index[index].Term, index);
        return true;
    }

    /// <summary>
    /// Очистить лог полностью и обновить индекс коммита
    /// </summary>
    public void Clear()
    {
        if (_index.Count == 0)
        {
            return;
        }

        // Обновляем индекс коммита
        _logFileStream.Seek(CommitIndexPosition, SeekOrigin.Begin);
        var writer = new StreamBinaryWriter(_logFileStream);
        writer.Write(NoCommittedEntriesIndex);
        CommitIndex = NoCommittedEntriesIndex;

        // Очищаем содержимое файла лога
        writer.Write(EndOfDataRecordMarker);
        _index.Clear();
    }

    internal void ValidateFileTest()
    {
        _ = Initialize(_logFileStream);
    }
}