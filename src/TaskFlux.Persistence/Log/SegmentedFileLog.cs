﻿using System.Buffers;
using System.Diagnostics;
using System.IO.Abstractions;
using Serilog;
using Serilog.Core;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Persistence.Log;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;
using ArgumentOutOfRangeException = System.ArgumentOutOfRangeException;
using Constants = TaskFlux.Consensus.Persistence.Constants;
using IOException = System.IO.IOException;

namespace TaskFlux.Persistence.Log;

public class SegmentedFileLog : IDisposable
{
    /// <summary>
    /// Маркер файла сегмента лога.
    /// Находится в самом начале файла.
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
    private const int EndOfDataRecordMarker = 0x00000000;

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
    /// Константа, используемая во время чтения данных из сегментов, для уведомления,
    /// что размер прочитанных данных проверять не надо.
    /// </summary>
    private const long NoSizeCheck = -1;

    /// <summary>
    /// Лок для разделения доступов для записи/чтения лога.
    /// </summary>
    private readonly ReaderWriterLockSlim _lock = new();

    /// <summary>
    /// Логгер
    /// </summary>
    private readonly ILogger _logger;

    /// <summary>
    /// Директория с файлами лога
    /// </summary>
    private readonly IDirectoryInfo _logDirectory;

    /// <summary>
    /// Директория с закрытыми файлами лога.
    /// Запись в них закончена и запрещена, только для чтения.
    /// </summary>
    private readonly List<LogSegment> _sealed;

    /// <summary>
    /// Последний сегмент лога, использующийся для записи.
    /// </summary>
    private LogSegment _tail;

    private readonly SegmentedFileLogOptions _options;

    /// <summary>
    /// Индекс закоммиченной записи
    /// </summary>
    public Lsn CommitIndex { get; private set; }

    /// <summary>
    /// Класс, представляющий файл сегмента лога.
    /// Может быть как закрытым (законченным для записи), так и для открытым (хвостом).
    /// </summary>
    private class LogSegment
    {
        /// <summary>
        /// Размер буфера, который используется в <see cref="BufferedStream"/> при записи и чтении из основного файла сегмента
        /// </summary>
        private static int BufferSize => Environment.SystemPageSize;

        private LogSegment(SegmentFileName fileName,
            IFileInfo file,
            List<LogRecord> records,
            FileSystemStream? writeStream,
            ILogger logger)
        {
            _logger = logger;
            FileName = fileName;
            File = file;
            Records = records;

            if (writeStream is not null)
            {
                WriteStreamData = (writeStream, new BufferedStream(writeStream, BufferSize));
            }
            else
            {
                WriteStreamData = null;
            }
        }

        /// <summary>
        /// Название файла сегмента
        /// </summary>
        public SegmentFileName FileName { get; }

        /// <summary>
        /// Файл, который соответствует этому сегменту
        /// </summary>
        public IFileInfo File { get; }

        /// <summary>
        /// Индекс записей из лога для быстрого доступа до нужной записи.
        /// Если сегмент является хвостом (для записи), то в поле <see cref="LogRecord.Payload"/> содержатся данные записи,
        /// иначе (если сегмент закрыт), то это поле пусто.
        /// </summary>
        /// 
        private List<LogRecord> Records { get; }

        /// <summary>
        /// Последний индекс из лога.
        /// Указывает на реальный индекс лога и может быть меньше <see cref="LastRecordIndex"/> если лог пуст -
        /// это помогает обрабатывать ситуации, когда сегмент единственный (тогда индекс равен Tomb), когда есть несколько сегментов и т.д.
        /// </summary>
        public Lsn LastRecordIndex => LogStartIndex + Records.Count - 1;

        private readonly ILogger _logger;

        /// <summary>
        /// Индекс, начиная с которого хранятся записи
        /// </summary>
        public Lsn LogStartIndex => FileName.StartLsn;

        /// <summary>
        /// Поток файла для записи.
        /// Если равен null, то этот сегмент уже закрыт и используется только для записи.
        /// </summary>
        private (FileSystemStream FileStream, BufferedStream Buffered)? WriteStreamData { get; set; }

        /// <summary>
        /// Количество записей в сегменте
        /// </summary>
        public int Count => Records.Count;

        /// <summary>
        /// Перезаписать файл сегмента указанными в <paramref name="entries"/> записями (с индекса <paramref name="startIndex"/>).
        /// Запись производится до достижения жесткого предела размера файла (<paramref name="hardLimit"/>).
        /// <paramref name="listStartIndex"/> указывает на LSN записи, с которой начинаются записи в <paramref name="entries"/> (без учета <paramref name="startIndex"/>)
        /// </summary>
        /// <param name="entries">Записи, которые нужно записать</param>
        /// <param name="startIndex">Индекс, начиная с которого нужно читать записи из <paramref name="entries"/></param>
        /// <param name="listStartIndex">Индекс, начиная с которого производить запись. Указывается индекс первой записи в списке <paramref name="entries"/></param>
        /// <param name="hardLimit">Жесткий предел размера файла лога</param>
        /// <returns>Количество сделанных записей</returns>
        public int InsertRangeOverwrite(IReadOnlyList<LogEntry> entries,
            int startIndex,
            Lsn listStartIndex,
            long hardLimit)
        {
            Debug.Assert(WriteStreamData != null, "WriteStream != null",
                "Файл сегмента закрыт. Нельзя перезаписывать часть файла");
            Debug.Assert(entries.Count > 0, "Entries.Count > 0", "Зачем передавать пустой массив записей?");
            Debug.Assert(startIndex < entries.Count, "startIndex < entries.Count",
                "Индекс начала записи должен быть меньше количества записей");
            var count = entries.Count - startIndex;
            Debug.Assert(count > 0, "count > 0", "Зачем передавать пустой диапазон записей?");

            var actualStartLsn = listStartIndex + startIndex;
            if (LastRecordIndex + 1 < actualStartLsn)
            {
                throw new ArgumentOutOfRangeException(nameof(listStartIndex), listStartIndex,
                    $"Индекс записи превышает размер сегмента. Текущий размер сегмента: {Records.Count}");
            }

            /*
             * Тут могут быть 2 случая:
             * 1. Записать в конец файла - index = _index.Count
             * 2. Перезаписать существующие записи - index < _index.Count
             */

            if (actualStartLsn == LastRecordIndex + 1)
            {
                /*
                 * Указан индекс следующий после последнего - нужно добавить в конец файла.
                 * Эта формула работает, даже если в сегменте записей нет.
                 */
                return AppendRange(entries, startIndex, hardLimit);
            }

            // В противном случае, индекс указывает куда-то внутрь лога.
            // Нужно найти позицию, с которой необходимо произвести перезапись
            var localIndex = (long)(actualStartLsn - LogStartIndex);

            var (fileStream, bufferedStream) = WriteStreamData.Value;
            // Лог точно не пуст, т.к. если он пуст, то единственное доступное решение - добавить запись в конец (выше)
            var startPosition = Records[(int)localIndex].Position;

            bufferedStream.Position = startPosition;
            var writer = new StreamBinaryWriter(bufferedStream);

            var newIndexValues = new List<LogRecord>(entries.Count);
            for (var i = startIndex; i < entries.Count; i++)
            {
                var entry = entries[i];
                var recordStartPosition = bufferedStream.Position;
                AppendRecordCore(entry, ref writer);
                newIndexValues.Add(new LogRecord(entry.Term, entry.GetCheckSum(), entry.Data, entry.Data.Length,
                    recordStartPosition));
                if (hardLimit <= bufferedStream.Position)
                {
                    // Достигли жесткого предела
                    break;
                }
            }

            writer.Write(EndOfDataRecordMarker);

            bufferedStream.Flush();
            fileStream.Fsync();

            // Обновляем индекс
            Records.RemoveRange((int)localIndex, (int)(Records.Count - localIndex));
            Records.AddRange(newIndexValues);

            return newIndexValues.Count;
        }

        public bool Contains(Lsn index)
        {
            return LogStartIndex <= index && index <= LastRecordIndex;
        }

        /// <summary>
        /// Запись новую запись в конец файла лога без коммита
        /// </summary>
        /// <param name="entry">Запись, которую нужно добавить</param>
        /// <returns>LSN новой добавленной записи. Указывается глобальный</returns>
        public Lsn Append(LogEntry entry)
        {
            Debug.Assert(WriteStreamData != null, "WriteStream != null",
                "Файл сегмента закрыт. Нельзя дозаписывать запись в конец");
            var (fileStream, bufferedStream) = WriteStreamData.Value;
            var savedAppendPosition = bufferedStream.Seek(GetAppendPosition(), SeekOrigin.Begin);

            var writer = new StreamBinaryWriter(bufferedStream);

            AppendRecordCore(entry, ref writer);

            writer.Write(EndOfDataRecordMarker);

            bufferedStream.Flush();
            fileStream.Fsync();

            Records.Add(new LogRecord(entry.Term, entry.GetCheckSum(), entry.Data, entry.Data.Length,
                savedAppendPosition));

            return LogStartIndex + Records.Count - 1;
        }

        /// <summary>
        /// Позиция, начиная с которой необходимо записывать в лог новые элементы.
        /// Если лог пустой, то указывает на <see cref="DataStartPosition"/>,
        /// в противном случае - на конец последней записи, прямо на начало <see cref="EndOfDataRecordMarker"/>.
        /// Т.е. при дозаписи необходимо позиционироваться на эту позицию.
        /// </summary>
        private long GetAppendPosition() =>
            Records.Count == 0
                ? DataStartPosition
                : Records[^1].GetNextRecordPosition();

        private static void AppendRecordCore(LogEntry entry, ref StreamBinaryWriter writer)
        {
            writer.Write(RecordMarker);
            writer.Write(entry.Term);
            writer.Write(entry.GetCheckSum());
            writer.WriteBuffer(entry.Data);
        }

        /// <summary>
        /// Получить информацию о записи по указанному индексу. 
        /// </summary>
        /// <param name="index">Индекс записи, которую нужно получить. Индекс глобальный</param>
        /// <returns>Информация о требуемой записи</returns>
        public LogEntryInfo GetInfoAt(Lsn index)
        {
            var localIndex = (int)(index - LogStartIndex);
            var record = Records[localIndex];
            return new LogEntryInfo(record.Term, index);
        }

        public bool TryGetLastEntry(out LogEntryInfo lastEntryInfo)
        {
            if (Records.Count > 0)
            {
                var index = Records.Count - 1;
                var last = Records[index];
                lastEntryInfo = new LogEntryInfo(last.Term, LogStartIndex + index);
                return true;
            }

            lastEntryInfo = LogEntryInfo.Tomb;
            return false;
        }

        /// <summary>
        /// Получить текущий размер файла, занятый данными записей.
        /// Не стоит использовать сам размер файла (<see cref="IFileInfo.Length"/>),
        /// т.к. файл изначально может быть создан с выделенным заранее размером
        /// </summary>
        /// <returns>Занятый данными размер файла</returns>
        public long GetEffectiveFileSize()
        {
            return Records.Count > 0
                ? Records[^1].GetNextRecordPosition()
                : DataStartPosition + sizeof(int); // Меньше чем заголовок + маркер окончания быть не может 
        }

        /// <summary>
        /// Записать в конец файла несколько записей без коммита
        /// </summary>
        /// <param name="entries">Записи, которые необходимо записать</param>
        /// <param name="startIndex">Индекс, с которого читать записи из <paramref name="startIndex"/></param>
        /// <param name="hardLimit">Жесткий предел размера сегмента</param>
        /// <returns>Количество сделанных записей</returns>
        private int AppendRange(IReadOnlyList<LogEntry> entries, int startIndex, long hardLimit)
        {
            Debug.Assert(WriteStreamData != null, "WriteStream != null",
                "Файл сегмента закрыт. Нельзя дозаписывать записи в конец");
            Debug.Assert(entries.Count > 0, "entries.Count > 0", "Не нужно записывать пустой массив. Проверяй выше");

            var count = entries.Count - startIndex;
            Debug.Assert(count > 0, "count > 0", "Зачем записывать пустой массив");

            var (fileStream, bufferedStream) = WriteStreamData.Value;

            var writer = new StreamBinaryWriter(bufferedStream);
            var startPosition = GetAppendPosition();

            bufferedStream.Seek(startPosition, SeekOrigin.Begin);
            var appended = 0;
            for (var i = startIndex; i < entries.Count; i++)
            {
                var entry = entries[i];
                var recordStartPosition = bufferedStream.Position;
                AppendRecordCore(entry, ref writer);
                Records.Add(new LogRecord(entry.Term, entry.GetCheckSum(), entry.Data, entry.Data.Length,
                    recordStartPosition));
                appended++;
                if (hardLimit <= bufferedStream.Position)
                {
                    // Превышен жесткий предел размера сегмента
                    break;
                }
            }

            writer.Write(EndOfDataRecordMarker);

            bufferedStream.Flush();
            fileStream.Fsync();

            return appended;
        }

        /// <summary>
        /// Прочитать из лога все записи в указанном диапазоне
        /// </summary>
        /// <param name="start">Начальный индекс записи включительно</param>
        /// <param name="end">Последний индекс записи включительно</param>
        /// <param name="allowSize">Допустимый размер занимаемых записей</param>
        /// <param name="written">Размер записанных данных</param>
        /// <returns>Список прочитанных записей</returns>
        public IReadOnlyList<LogEntry> ReadRange(Lsn start, Lsn end, in long allowSize, out long written)
        {
            Debug.Assert(allowSize is > 0 or NoSizeCheck, "allowSize is > 0 or NoSizeCheck",
                "Разрешенный размер прочитанных записей должен быть положительным либо равен NoSizeCheck константе - переданный равен {0}",
                allowSize);

            var localStart = (int)(start - LogStartIndex);
            var localEnd = (int)(end - LogStartIndex);

            if (localStart < 0)
            {
                throw new InvalidOperationException($"Индекса {start} нет в сегменте {FileName}");
            }

            if (Records.Count <= localEnd)
            {
                throw new InvalidOperationException($"Индекса {end} нет в сегменте {FileName}");
            }

            var count = localEnd - localStart + 1;
            var entries = new List<LogEntry>(count);
            var leftSize = allowSize;
            var shouldCheckSize = allowSize != NoSizeCheck;
            if (WriteStreamData is not null)
            {
                // Хвост - данные находятся в индексе
                for (var i = localStart; i <= localEnd; i++)
                {
                    var record = Records[i];
                    var entry = new LogEntry(record.Term, record.Payload);
                    entry.SetCheckSum(record.CheckSum);
                    entries.Add(entry);
                    if (shouldCheckSize)
                    {
                        leftSize -= entry.CalculateRecordSize();
                        if (leftSize <= 0)
                        {
                            break;
                        }
                    }
                }
            }
            else
            {
                _logger.Information("Открываю сегмент {SegmentFileName} для чтения", File.Name);
                // Сегмент закрыт - данные нужно прочитать из файла
                using var file = File.OpenRead();
                var reader = new StreamBinaryReader(file);
                for (int i = localStart; i <= localEnd; i++)
                {
                    var record = Records[i];
                    var readPosition = record.GetCheckSumPosition();
                    file.Seek(readPosition, SeekOrigin.Begin);
                    var storedCheckSum = reader.ReadUInt32();
                    // После чек-суммы идет буфер с данными
                    var payload = reader.ReadBuffer();
                    var calculatedCheckSum = Crc32CheckSum.Compute(payload);
                    if (calculatedCheckSum != storedCheckSum)
                    {
                        throw new InvalidDataException(
                            $"Рассчитанная чек-сумма не равна сохраненной. Сегмент {FileName.GetFileName()}, индекс {LogStartIndex + i}, позиция в файле {record.Position}");
                    }

                    var entry = new LogEntry(record.Term, payload);
                    entry.SetCheckSum(calculatedCheckSum);

                    entries.Add(entry);

                    if (shouldCheckSize)
                    {
                        leftSize -= entry.CalculateRecordSize();
                        if (leftSize <= 0)
                        {
                            break;
                        }
                    }
                }
            }

            written = shouldCheckSize
                ? allowSize - leftSize
                : -1;
            return entries;
        }

        /// <summary>
        /// Прочитать все записи, начиная с указанного индекса
        /// </summary>
        /// <param name="start">Индекс, начиная с которого необходимо начинать чтение включительно. Индекс глобальный</param>
        /// <param name="allowSize">Допустимый размер занимаемых записей</param>
        /// <param name="written">Размер записанных данных</param>
        /// <returns>Хранившиеся записи, начиная с указанной позиции</returns>
        public IReadOnlyList<LogEntry> ReadFrom(Lsn start, in long allowSize, out long written)
        {
            return ReadRange(start, LastRecordIndex, allowSize, out written);
        }

        /// <summary>
        /// Прочитать все записи из сегмента
        /// </summary>
        /// <returns></returns>
        public IReadOnlyList<LogEntry> ReadAll(long allowSize, out long written)
        {
            if (Records.Count == 0)
            {
                written = 0;
                return Array.Empty<LogEntry>();
            }

            return ReadRange(LogStartIndex, LastRecordIndex, allowSize, out written);
        }

        private static LogSegment InitializeCore(IFileInfo fileInfo,
            SegmentFileName fileName,
            SegmentedFileLogOptions options,
            ILogger logger,
            bool readOnly)
        {
            // Файл открываем в ReadWrite режиме, т.к. может потребоваться восстановление файла (обрезание)
            logger.Debug("Открываю файл {FileName} для чтения и записи", fileInfo.Name);
            var file = fileInfo.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

            var reader = new StreamBinaryReader(file);

            if (file.Length == 0)
            {
                // Файл был пуст
                Span<byte> headerSpan = stackalloc byte[HeaderSizeBytes + sizeof(int)]; // Заголовок + EndOfDataRecord

                if (options.PreallocateSegment)
                {
                    logger.Debug("Файл {SegmentFileName} пуст - инициализирую заголовок", fileInfo.Name);
                    file.SetLength(options.LogFileSoftLimit);
                }

                var headerWriter = new SpanBinaryWriter(headerSpan);
                headerWriter.Write(FileMarker);
                headerWriter.Write(CurrentVersion);
                headerWriter.Write(EndOfDataRecordMarker);

                file.Write(headerSpan);
                file.Fsync();
                if (readOnly)
                {
                    file.Dispose();
                    file = null;
                }

                return new LogSegment(fileName, fileInfo, new List<LogRecord>(), file, logger);
            }

            logger.Debug("Начинаю валидацию заголовка сегмента {SegmentFileName}", fileInfo.Name);
            file.Seek(0, SeekOrigin.Begin);

            // Валидация заголовка файла
            try
            {
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
            }
            catch (EndOfStreamException)
            {
                logger.Information(
                    "Размер сегмента {FileName} меньше размера заголовка - выполняю первоначальное инициализирование",
                    fileName.GetFileName());

                if (options.PreallocateSegment)
                {
                    // В стандартном API не нашел функции fallocate (или подобной)
                    logger.Debug("Выделяю память под файл сегмента {SegmentFileName}", file.Name);
                    file.SetLength(options.LogFileSoftLimit);
                }

                const int requiredSize = HeaderSizeBytes + sizeof(int);
                var buffer = ArrayPool<byte>.Shared.Rent(requiredSize);
                try
                {
                    var span = buffer.AsSpan(0, requiredSize);
                    var writer = new SpanBinaryWriter(span);
                    writer.Write(FileMarker);
                    writer.Write(CurrentVersion);
                    writer.Write(EndOfDataRecordMarker);

                    file.Write(span);
                    file.Fsync();

                    if (readOnly)
                    {
                        file.Dispose();
                        file = null;
                    }

                    return new LogSegment(fileName, fileInfo, new List<LogRecord>(), file, logger);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }

            logger.Debug("Начинаю валидацию содержимого сегмента {SegmentFileName}", fileInfo.Name);
            var index = new List<LogRecord>();
            var recordStartPosition = file.Position;
            var success = true;

            // Воссоздаем индекс записей сегмента
            try
            {
                /*
                 * Процесс простой: пытаемся прочитать корректную запись до наступления одного из следующих условий:
                 * - Встретился маркер конца файла
                 * - Достигнут конец файла
                 * - Подсчитанная чек-сумма не равна хранившейся
                 *
                 * В индекс добавляются все записи, которые были корректно прочитаны.
                 * В случае, если было обнаружено нарушение целостности, то файл обрезается до последней корректной записи.
                 */

                // ! Не выносить в цикл, т.к. этот спан будет создаваться новый на каждую итерацию
                Span<byte> buffer = stackalloc byte[BufferSize];

                // Пока не достигли конца файла
                while ((recordStartPosition = file.Position) < file.Length)
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
                    byte[] data;
                    int dataLength;

                    if (readOnly)
                    {
                        // Проверяем корректность данных
                        dataLength = reader.ReadInt32();

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
                                catch (EndOfStreamException)
                                {
                                    logger.Warning(
                                        "Во время инициализации сегмента {FileName} обнаружен неожиданный конец файла",
                                        fileName.GetFileName());
                                    success = false;
                                    break;
                                }

                                computedCheckSum = Crc32CheckSum.Compute(computedCheckSum, span);
                                left -= span.Length;
                            } while (left > 0);
                        }

                        data = Array.Empty<byte>();
                    }
                    else
                    {
                        data = reader.ReadBuffer();
                        computedCheckSum = Crc32CheckSum.Compute(data);
                        dataLength = data.Length;
                    }

                    if (storedCheckSum != computedCheckSum)
                    {
                        logger.Warning(
                            "Во время инициализации сегмента {FileName} обнаружено нарушение целостности: рассчитанная чек-сумма не равна сохраненной",
                            fileName.GetFileName());
                        success = false;
                        break;
                    }

                    index.Add(new LogRecord(term, computedCheckSum, data, dataLength, recordStartPosition));
                }
            }
            catch (EndOfStreamException)
            {
                logger.Warning("Во время инициализации сегмента {FileName} обнаружен неожиданный конец файла",
                    fileName.GetFileName());
                success = false;
            }

            // Если была обнаружена ошибка при прочтении файла
            if (!success)
            {
                var resultLength =
                    recordStartPosition + sizeof(int); // Индекс начала записи + Маркер окончания (4 байта)
                logger.Information("Обрезаю файл {FileName} до {Length}. Сохраняю записи до {Lsn} индекса",
                    fileName.GetFileName(), resultLength, fileName.StartLsn + index.Count - 1);

                // Обрезаем файл и выставляем маркер конца
                file.SetLength(resultLength);
                file.Seek(sizeof(int), SeekOrigin.End);
                Span<byte> s = stackalloc byte[sizeof(int)];
                new SpanBinaryWriter(s).Write(EndOfDataRecordMarker);
                file.Write(s);
                file.Fsync();
            }

            if (readOnly)
            {
                file.Close();
                file.Dispose();
                file = null;
            }

            return new LogSegment(fileName, fileInfo, index, file, logger);
        }

        /// <summary>
        /// Открыть указанный файл и инициализировать его в ReadOnly режиме.
        /// При необходимости, содержимое файла лога восстанавливается при обнаружении нарушении целостности.
        /// </summary>
        /// <returns>Инициализированный файл сегмента лога</returns>
        public static LogSegment InitializeSealed(IFileInfo fileInfo,
            SegmentFileName fileName,
            SegmentedFileLogOptions options,
            ILogger logger)
        {
            return InitializeCore(fileInfo, fileName, options, logger, true);
        }

        /// <summary>
        /// Инициализировать файл сегмента в Write режиме 
        /// </summary>
        public static LogSegment InitializeTail(IFileInfo fileInfo,
            SegmentFileName fileName,
            SegmentedFileLogOptions options,
            ILogger logger)
        {
            return InitializeCore(fileInfo, fileName, options, logger, false);
        }

        public void Seal()
        {
            Debug.Assert(WriteStreamData is not null, "WriteStream is not null",
                "Если нужно закрыть сегмент, то он обязан быть открыт прежде");
            // Закрываем поток файлаa
            _logger.Debug("Запечатываю сегмент {SegmentFileName}", FileName.GetFileName());
            var (writeStream, bufferedStream) = WriteStreamData.Value;
            bufferedStream.Flush();
            writeStream.Close();

            WriteStreamData = null;

            // Очищаем все хранившиеся данные записи и индексе
            Records.ForEach(i => i.ClearPayload());
        }

        public void MakeTail()
        {
            Debug.Assert(WriteStreamData is null, "WriteStream is null",
                "Если нужно открыть сегмент, то он обязан быть закрыт прежде");

            // Если указанный лог нужно сделать хвостом, то необходимо открыть файл для записи
            // и восстановить индекс файла

            _logger.Debug("Открываю сегмент {SegmentName} для чтения и записи", File.Name);
            var fileStream = File.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            try
            {
                // В индексе уже должны храниться данные для восстановления данных в индексе: позиция начала записи в файле
                var bufferedStream = new BufferedStream(fileStream, BufferSize);
                var reader = new StreamBinaryReader(bufferedStream);
                // Все записи в индексе расположены последовательно
                foreach (var record in Records)
                {
                    bufferedStream.Seek(record.GetDataStartPosition(), SeekOrigin.Begin);
                    var buffer = reader.ReadBuffer();
                    var computedCheckSum = Crc32CheckSum.Compute(buffer);
                    if (computedCheckSum != record.CheckSum)
                    {
                        throw new InvalidDataException(
                            "Рассчитанная из файла чек-сумма не равна сохраненной в приложении");
                    }

                    record.SetPayload(buffer);
                }

                WriteStreamData = (fileStream, bufferedStream);
            }
            catch (Exception)
            {
                fileStream.Dispose();
                WriteStreamData = null;
                throw;
            }
        }

        /// <summary>
        /// Удалить указанный файл сегмента и очистить все состояние.
        /// После этой операции работать с сегментом запрещено, т.к. сам файл уже удален
        /// </summary>
        public void Delete()
        {
            if (WriteStreamData is var (stream, buffered))
            {
                // Закрываем поток файла
                _logger.Debug("Закрываю файл сегмента {FileName}", File.FullName);
                buffered.Flush();
                stream.Close();

                // Данные потока удаляем
                WriteStreamData = null;
            }

            _logger.Information("Удаляю файл сегмента {FileName}", File.FullName);
            // Удаляем сам файл
            File.Delete();

            // Очищаем индекс файла
            Records.Clear();
        }

        public void SetupTest(IReadOnlyList<LogEntry> entries)
        {
            // Если сегмент был закрыт, то нужно самим открыть и закрыть файл сегмента
            var owsStream = false;
            Stream stream;
            if (WriteStreamData is var (_, s))
            {
                stream = s;
            }
            else
            {
                stream = File.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                owsStream = true;
            }

            stream.Position = DataStartPosition;
            Records.Clear();
            var writer = new StreamBinaryWriter(stream);
            foreach (var entry in entries)
            {
                var startPosition = stream.Position;
                AppendRecordCore(entry, ref writer);
                Records.Add(
                    new LogRecord(entry.Term, entry.GetCheckSum(), entry.Data, entry.Data.Length, startPosition));
            }

            stream.Flush();

            if (owsStream)
            {
                stream.Dispose();
            }
        }

        public void Close()
        {
            if (WriteStreamData is var (s, b))
            {
                b.Flush();
                s.Fsync();
                s.Close();

                WriteStreamData = null;
            }
        }
    }

    /// <summary>
    /// Количество записей в логе
    /// </summary>
    public long Count => LastRecordIndex - StartIndex + 1;

    /// <summary>
    /// Индекс последней записи лога.
    /// Может быть Tomb, если лог пуст
    /// </summary>
    public Lsn LastRecordIndex => _tail.LastRecordIndex;

    /// <summary>
    /// Индекс, с которого начинаются все записи в логе.
    /// Записи с этим индексом может и не существовать
    /// </summary>
    public Lsn StartIndex => _sealed.Count > 0
        ? _sealed[0].LogStartIndex
        : _tail.LogStartIndex;

    private SegmentedFileLog(IDirectoryInfo logDirectory,
        List<LogSegment> sealedSegments,
        LogSegment tail,
        SegmentedFileLogOptions options,
        ILogger logger)
    {
        _logDirectory = logDirectory;
        _sealed = sealedSegments;
        _tail = tail;
        _options = options;
        _logger = logger;
        CommitIndex = StartIndex - 1;
    }

    public static SegmentedFileLog Initialize(IDirectoryInfo dataDirectory,
        ILogger logger,
        SegmentedFileLogOptions options)
    {
        var logDirectory = GetLogDirectory();

        if (!logDirectory.Exists)
        {
            // Если директории с сегментами лога не существовала, то создаем новую и инициализируем первый файл.
            // Ничего больше не надо
            logger.Information("Создаю директорию для файлов лога {LogFileDirectory}", logDirectory.FullName);

            try
            {
                logDirectory.Create();
            }
            catch (Exception e)
            {
                throw new IOException("Ошибка при создании директории для сегментов лога", e);
            }

            var firstSegmentName = new SegmentFileName(0);
            var firstSegmentFile = GetSegmentFileInfo(logDirectory, firstSegmentName);

            logger.Debug("Инициализирую первый файл сегмента {SegmentFileName}", firstSegmentFile.FullName);
            var firstTailSegment = LogSegment.InitializeTail(firstSegmentFile, firstSegmentName, options, logger);

            return new SegmentedFileLog(logDirectory, new List<LogSegment>(), firstTailSegment, options, logger);
        }

        var possibleSegmentFiles = GetPossibleSegmentFiles(logDirectory);

        // Инициализируем первый файл сегмента, т.к. директория лога была пуста
        if (possibleSegmentFiles.Count == 0)
        {
            var firstSegmentName = new SegmentFileName(0);
            var firstSegmentFile = GetSegmentFileInfo(logDirectory, firstSegmentName);
            logger.Information(
                "В директории сегментов не оказалось файлов сегментов. Инициализирую первый сегмент {FileName}",
                firstSegmentFile.Name);

            var firstTailSegment = LogSegment.InitializeTail(firstSegmentFile, firstSegmentName, options, logger);

            return new SegmentedFileLog(logDirectory, new List<LogSegment>(), firstTailSegment, options, logger);
        }

        // Если сегмент только 1, то и инициализируем только его
        if (possibleSegmentFiles.Count == 1)
        {
            var (fileInfo, fileName) = possibleSegmentFiles[0];
            var segmentFile = LogSegment.InitializeTail(fileInfo, fileName, options, logger);
            return new SegmentedFileLog(logDirectory, new List<LogSegment>(), segmentFile, options, logger);
        }

        // Если есть существующие файлы сегментов, то каждый инициализируем.
        // Дополнительно проверяем, что целостность была не нарушена - если нарушена, удаляем все невалидные файлы.
        possibleSegmentFiles.Sort(static (left, right) =>
            left.FileName.StartLsn.Value.CompareTo(right.FileName.StartLsn.Value));

        var allSegments = new List<LogSegment>(possibleSegmentFiles.Count);
        /*
         * На каждом шаге проверяем, что начальный индекс лога на один больше последнего индекса предшествующего лога:
         * prevLogSegment.LastIndex == currentSegment.StartIndex + 1
         */

        LogSegment? prevLogSegment = null;
        for (var i = 0; i < possibleSegmentFiles.Count; i++)
        {
            var (fileInfo, name) = possibleSegmentFiles[i];
            var segmentFile = LogSegment.InitializeSealed(fileInfo, name, options, logger);

            if (prevLogSegment is { } pls && pls.LastRecordIndex + 1 != segmentFile.LogStartIndex)
            {
                /*
                 * В нормальном состоянии LSN начала и конца образуют непрерывную последовательность.
                 * Если все в порядке, то последний LSN предыдущего файла + 1 == первый LSN нового файла.
                 * В противном случае, целостность была нарушена и необходимо удалить остальные сегменты.
                 *
                 * Также возможен случай, когда кол-во записей в сегменте равно 0.
                 * Единственный допустимый случай этого - сегмент последний, т.е. хвост.
                 */

                logger.Warning(
                    "Обнаружено нарушение целостности: сегмент {CurrentSegmentFileName} имеет начальный индекс {StartIndex}, но последний индекс в сегменте {PreviousFileName} - {LastSegment}",
                    segmentFile.FileName, segmentFile.LogStartIndex, prevLogSegment.FileName,
                    prevLogSegment.LastRecordIndex);

                var invalidFiles = possibleSegmentFiles.GetRange(i, possibleSegmentFiles.Count - i);
                for (var j = invalidFiles.Count - 1; j >= 0; j--)
                {
                    var file = invalidFiles[j].File;
                    logger.Information("Удаляю файл {FileName}", file.FullName);
                    file.Delete();
                }

                break;
            }

            prevLogSegment = segmentFile;
            allSegments.Add(segmentFile);
        }

        Debug.Assert(allSegments.Count > 0, "allSegments.Count > 0", "Как минимум 1 сегмент должен присутствовать");

        // Теперь, последний сегмент делаем хвостом, а остальные оставляем закрытыми.
        // На данный момент, у нас гарантированно есть как минимум 1 сегмент.
        var sealedSegments = allSegments[..^1];
        var tail = allSegments[^1];
        tail.MakeTail();
        return new SegmentedFileLog(logDirectory, sealedSegments, tail, options, logger);

        IDirectoryInfo GetLogDirectory()
        {
            return dataDirectory.FileSystem.DirectoryInfo.New(Path.Combine(dataDirectory.FullName,
                Constants.LogDirectoryName));
        }

        List<(IFileInfo File, SegmentFileName FileName)> GetPossibleSegmentFiles(IDirectoryInfo logDir)
        {
            var possiblyLogSegmentFiles = logDir.GetFiles("*.log", SearchOption.TopDirectoryOnly);
            var files = new List<(IFileInfo File, SegmentFileName FileName)>();
            foreach (var file in possiblyLogSegmentFiles)
            {
                if (SegmentFileName.TryParse(file.Name, out var segmentName))
                {
                    files.Add((file, segmentName));
                }
            }

            return files;
        }
    }

    internal static SegmentedFileLog InitializeTest(IDirectoryInfo logDirectory,
        Lsn startIndex,
        IReadOnlyList<LogEntry>? tailEntries = null,
        IReadOnlyList<IReadOnlyList<LogEntry>>? segmentEntries = null,
        SegmentedFileLogOptions? options = null)
    {
        Debug.Assert(!startIndex.IsTomb, "!startIndex.IsTomb", "Индекс записи может начинаться только с 0");
        if (!logDirectory.Exists)
        {
            logDirectory.Create();
        }

        var sealedSegments = new List<LogSegment>();
        var index = startIndex;
        if (segmentEntries is { Count: > 0 })
        {
            foreach (var entries in segmentEntries)
            {
                Debug.Assert(entries.Count > 0, "entries.Count > 0", "Закрытый сегмент обязан иметь данные");
                var fileName = new SegmentFileName(index);
                var fileInfo = GetSegmentFileInfo(logDirectory, fileName);
                var segment = LogSegment.InitializeSealed(fileInfo, fileName, GetOptions(), Logger.None);
                segment.SetupTest(entries);
                sealedSegments.Add(segment);
                index += entries.Count;
            }
        }

        var tailFileName = new SegmentFileName(index);
        var tailFileInfo = GetSegmentFileInfo(logDirectory, tailFileName);
        var tail = LogSegment.InitializeTail(tailFileInfo, tailFileName, GetOptions(), Logger.None);
        if (tailEntries is { Count: > 0 })
        {
            tail.SetupTest(tailEntries);
        }

        return new SegmentedFileLog(logDirectory, sealedSegments, tail, options ?? SegmentedFileLogOptions.Default,
            Logger.None);

        SegmentedFileLogOptions GetOptions()
        {
            return options ?? new SegmentedFileLogOptions(long.MaxValue, long.MaxValue, false, long.MaxValue);
        }
    }

    private static IFileInfo GetSegmentFileInfo(IDirectoryInfo logDirectory, SegmentFileName fileName)
    {
        return logDirectory.FileSystem.FileInfo.New(Path.Combine(logDirectory.FullName,
            fileName.GetFileName()));
    }

    /// <summary>
    /// Получить информацию о последней записи в логе.
    /// Индекс указывает на индекс записи в файле - не глобальный
    /// </summary>
    /// <returns>Последняя запись в логе. Может быть <see cref="LogEntryInfo.Tomb"/>, если записей нет</returns>
    public bool TryGetLastLogEntry(out LogEntryInfo lastEntry)
    {
        using var _ = BeginReadLock();
        if (_tail.TryGetLastEntry(out lastEntry))
        {
            return true;
        }

        if (_sealed.Count > 0)
        {
            var lastLog = _sealed.Count - 1;
            var last = _sealed[lastLog];
            var sealedContains = last.TryGetLastEntry(out lastEntry);
            Debug.Assert(sealedContains, "sealedContains", "Запечатанный лог не должен быть пуст");
            return true;
        }

        if (_tail.LogStartIndex == 0)
        {
            lastEntry = LogEntryInfo.Tomb;
            return true;
        }

        lastEntry = LogEntryInfo.Tomb;
        return false;
    }

    public LogEntryInfo GetEntryInfoAt(Lsn index)
    {
        _lock.EnterReadLock();
        try
        {
            var segment = GetSegmentContaining(index);
            return segment.GetInfoAt(index);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private LogSegment GetSegmentContaining(Lsn index)
    {
        /*
         * Поиск делаю исходя из предположения, что запросы будут приходить на наиболее актуальные записи.
         * Поэтому более эффективнее - просто последовательно с конца пролистать все сегменты.
         */

        if (_tail.LogStartIndex <= index)
        {
            if (_tail.LastRecordIndex < index)
            {
                throw new InvalidOperationException(
                    $"Индекс {index} выходит за пределы находящихся в логе записей. Последний индекс в логе: {_tail.LastRecordIndex}");
            }

            return _tail;
        }

        for (var i = _sealed.Count - 1; i >= 0; i--)
        {
            var s = _sealed[i];
            if (s.LogStartIndex <= index)
            {
                return s;
            }
        }

        throw new InvalidOperationException($"Сегмента, содержащего индекс {index.Value}, не найдено");
    }

    private bool TryGetSegmentContaining(Lsn index, out LogSegment segment)
    {
        if (_tail.LogStartIndex <= index)
        {
            if (_tail.LastRecordIndex < index)
            {
                segment = default!;
                return false;
            }

            segment = _tail;
            return true;
        }

        for (var i = _sealed.Count - 1; i >= 0; i--)
        {
            var s = _sealed[i];
            if (s.LogStartIndex <= index)
            {
                segment = s;
                return true;
            }
        }

        segment = default!;
        return false;
    }

    public void Dispose()
    {
        _lock.Dispose();
        foreach (var s in _sealed)
        {
            s.Close();
        }

        _tail.Close();
    }

    /// <summary>
    /// Метод для чтения всех записей из файла лога.
    /// </summary>
    /// <remarks>Используется для тестов</remarks>
    internal IReadOnlyList<LogEntry> ReadAllTest()
    {
        return _sealed.Append(_tail)
            .Aggregate(new List<LogEntry>(), (list, segment) =>
            {
                list.AddRange(segment.ReadAll(long.MaxValue, out _));
                return list;
            });
    }

    /// <summary>
    /// Получить все записи, начиная с указанного.
    /// Если <paramref name="index"/> указывает на индекс, принадлежавший удаленному сегменту, то возвращается <c>false</c>,
    /// в противном случае возвращается <c>true</c>.
    /// Если индекс больше последнего, то в <paramref name="entries"/> пустой массив и возвращается <c>true</c> - это нужно,
    /// чтобы обработчики узлов корректно обрабатывали ситуацию, когда NextIndex указывает на следующий после последнего
    /// </summary>
    /// <param name="index">Индекс, начиная с которого нужно получить все записи</param>
    /// <param name="entries">Записи, которые были прочитаны</param>
    /// <param name="prevLogEntry">Данные о записи, предшествующей записи по индексу <paramref name="index"/></param>
    /// <returns><c>true</c> - записи успешно прочитаны <c>false</c> - указан индекс до первой записи</returns>
    public bool TryGetFrom(Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevLogEntry)
    {
        // Индекс указывает на уже удаленный сегмент
        if (index < StartIndex)
        {
            entries = Array.Empty<LogEntry>();
            prevLogEntry = LogEntryInfo.Tomb;
            return false;
        }

        // Вначале выставляем предшествующий индекс и только после начинаем читать сами записи
        if (index == StartIndex)
        {
            // Когда индекс равен ПЕРВОЙ ЗАПИСИ в логе
            if (StartIndex == 0)
            {
                // ... то либо лог никогда не обрезался и возвращаем Tomb
                prevLogEntry = LogEntryInfo.Tomb;
            }
            else
            {
                // ... либо сегменты были обрезаны и данные об удаленной ранее уже недоступны
                prevLogEntry = LogEntryInfo.Tomb;
                entries = Array.Empty<LogEntry>();
                return false;
            }
        }
        else if (index == LastRecordIndex + 1)
        {
            // Когда индекс равен ПОСЛЕДНЕЙ ЗАПИСИ в логе
            if (0 < _tail.Count)
            {
                // ... то последняя запись либо в хвосте
                prevLogEntry = _tail.GetInfoAt(index - 1);
            }
            else
            {
                // ... либо в последнем закрытом сегменте
                prevLogEntry = _sealed[^1].GetInfoAt(index - 1);
            }

            // Нет смысла продолжать, т.к. индекс и так последний
            entries = Array.Empty<LogEntry>();
            return true;
        }
        else
        {
            // В противном случае, он будет где-то внутри лога
            var indexToFind = index - 1;
            if (_tail.Contains(indexToFind))
            {
                // ... то он либо в хвосте (быстро найдем)
                prevLogEntry = _tail.GetInfoAt(indexToFind);
            }
            else
            {
                // ... либо в одном из сегментов, для оптимизации будем искать с последнего сегмента
                var recordFound = false;
                prevLogEntry = LogEntryInfo.Tomb;
                for (var i = _sealed.Count - 1; i >= 0; i--)
                {
                    var segment = _sealed[i];
                    if (segment.Contains(indexToFind))
                    {
                        recordFound = true;
                        prevLogEntry = segment.GetInfoAt(indexToFind);
                        break;
                    }
                }

                Debug.Assert(recordFound, "recordFound", "Запись должна была быть найдена");
            }
        }

        if (LastRecordIndex < index)
        {
            // Если индекс равен следующему после 
            entries = Array.Empty<LogEntry>();
            return true;
        }

        if (_tail.LogStartIndex <= index)
        {
            entries = _tail.ReadFrom(index, _options.MaxReadEntriesSize, out _);
            return true;
        }

        var result = new List<LogEntry>();

        /*
         * Чтение записей можно разделить на 3 этапа:
         * 1. Пропускаем сегменты, которые вообще не содержат нужные записи
         * 2. Читаем записи из сегмента, который (возможно) частично содержит записи с нужного индекса
         * 3. Полностью читаем все записи из оставшихся сегментов
         */

        // Находим индекс, в котором содержатся записи 
        var segmentIndex = 0;
        var leftSize = _options.MaxReadEntriesSize;
        for (; segmentIndex < _sealed.Count; segmentIndex++)
        {
            var segment = _sealed[segmentIndex];
            if (segment.Contains(index))
            {
                result.AddRange(segment.ReadFrom(index, leftSize, out var written));
                leftSize -= written;
                segmentIndex++;
                break;
            }
        }

        if (leftSize <= 0)
        {
            entries = result;
            return true;
        }

        // Полностью читаем все записи из оставшихся сегментов
        for (; segmentIndex < _sealed.Count; segmentIndex++)
        {
            var segment = _sealed[segmentIndex];
            var read = segment.ReadAll(leftSize, out var written);
            Debug.Assert(read.Count > 0, "read.Count > 0", "Закрытый сегмент не может быть пустым");
            result.AddRange(read);
            leftSize -= written;
        }

        if (0 < leftSize && 0 < _tail.Count)
        {
            result.AddRange(_tail.ReadAll(leftSize, out _));
        }

        entries = result;
        return true;
    }

    /// <summary>
    /// Записать указанные записи в файл с перезаписью незакоммиченных записей.
    /// </summary>
    /// <param name="entries">Записи, которые необходимо записать</param>
    /// <param name="index">Индекс, начиная с которого необходимо записывать данные</param>
    public void InsertRangeOverwrite(IReadOnlyList<LogEntry> entries, Lsn index)
    {
        using var _ = BeginWriteLock();
        CheckCommitOverwrite(index);

        /*
         * При вставке записей могут возникнуть ситуации:
         * - index < StartIndex           - индекс указывает на несуществующие записи (сегменты уже удалены).
         *                                  StartIndex по умолчанию является индексом коммита, поэтому будет исключение
         * - index < Tail.StartIndex      - индекс указывает на какой-то закрытый сегмент.
         *                                  В этом случае:
         *                                      1. Находим сегмент, которому принадлежит этот индекс
         *                                      2. Поочередно, начиная с конца (в порядке LSN), удаляем старые сегменты
         *                                      3. Делаем его хвостом и производим запись
         *                                  Мы можем спокойно удалять сегменты, т.к. все записи в них незакоммичены + скорее всего и должны быть перезаписаны.
         *                                  Если это делать с конца, то можем наткнуться на ситуацию, когда 1) в нужный сегмент добавили такое-же кол-во записей,
         *                                  сколько и перезаписываем 2) происходит сбой ->
         *                                  3) лог в некорректном состоянии, т.к. сразу после новых записей идут старые незакоммиченные,
         *                                  которые работают со старыми данными, а будут работать с новыми
         * - index <= Tail.LastIndex      - индекс указывает на запись внутри хвоста.
         *                                  Тогда просто перезаписываем незакоммиченные записи.
         * - index == Tail.LastIndex + 1  - нужно добавить запись в конец хвоста
         * - Tail.LastIndex < index       - указанный индекс больше последнего в логе.
         *                                  Такое может возникнуть, когда мы сильно отстали от лидера, либо добавились в уже существующий кластер и пусты.
         *                                  В этом случае:
         *                                      1. Удаляем все сегменты
         *                                      2. Создаем новый сегмент хвоста с индексом из параметра
         *                                      3. Добавляем нужные записи
         *                                  Мы можем просто удалить все файлы сегментов потому что мы уже знаем, что отстали и лидером стать не сможем,
         *                                  т.е. все равно придется удалять текущие сегменты.
         *                                  Даже если произойдет сбой и будет удалена лишь часть сегментов, лидер все равно отправит запрос выполним удаление сегментов
         *
         * NOTE: вообще, стратегия сначала удалить сегменты, а потом делать (пере)запись просто проще реализовать
         * (единый метод для обрезания лога до нужного индекса)
         */

        // Удаляем все записи (и возможно сегменты) до указанного индекса (не включая).
        // Этот метод обрабатывает все ситуации, когда вставка должна производиться не в конец текущего хвоста:
        // - Удалить все сегменты лога и создать новый
        // - Откатиться до нужного закрытого сегмента
        // - Удалить часть данных из хвоста
        // 
        // После выхода, _tail будет указывать на сегмент, в который нужно производить запись 
        MaybeTruncateUntil(index);

        // Возможно, текущий хвост уже слишком большой и его нужно отрезать и начать новый.
        // Создание нового сегмента происходит лениво, при добавлении записей в уже заполненный лог, 
        // чтобы не заниматься этим при коммите (возможно больше лог расти и не будет)
        MaybeBeginNewSegment();

        var left = entries.Count;
        var startIndex = 0;
        while (0 < left)
        {
            var appended = _tail.InsertRangeOverwrite(entries, startIndex, index, _options.LogFileHardLimit);
            if (appended == left)
            {
                break;
            }

            // Если количество добавленных записей не равно количеству оставшихся - значит был достигнут жесткий предел для текущего хвоста
            BeginNewSegment();
            startIndex += appended;
            left -= appended;
        }
    }

    /// <summary>
    /// Проверить возможность выполнить запись по указанному индексу
    /// </summary>
    /// <param name="index">Индекс, с которого нужно делать перезапись</param>
    /// <exception cref="InvalidOperationException">Указанный индекс меньшие индекса коммита</exception>
    private void CheckCommitOverwrite(Lsn index)
    {
        if (index <= CommitIndex)
        {
            throw new InvalidOperationException(
                $"Нельзя перезаписать закоммиченные записи. Указан индекс записи: {index}. Индекс коммита: {CommitIndex}");
        }
    }

    /// <summary>
    /// Получить количество сегментов, которые идут до сегмента с указанным индексом.
    /// Считаются сегменты, которые полностью покрываются указанным индексом.
    /// Если указанный индекс меньше первого, то возвращается -1.
    /// </summary>
    /// <param name="index">Индекс, до которого нужно читать сегменты</param>
    /// <returns>Количество сегментов, которые покрываются индексом, или -1 если этих сегментов нет</returns>
    public int GetSegmentsBefore(Lsn index)
    {
        using var _ = BeginReadLock();
        if (index < StartIndex)
        {
            return -1;
        }

        if (_sealed.Count == 0)
        {
            return 0;
        }

        var count = 0;
        foreach (var segment in _sealed)
        {
            if (index < segment.LastRecordIndex)
            {
                break;
            }

            count++;
        }

        return count;
    }

    /// <summary>
    /// Обрезать лог (удалить записи) до указанного индекса.
    /// После выполнения, лог может быть: <br/>
    /// - Не изменен, если индекс меньше начального (т.е. ничего обрезать не надо) <br/>
    /// - Полностью очищен, если индекс больше последнего (тогда хвост будет инициализирован <br/>
    /// </summary>
    /// <param name="index">Индекс, до которого нужно очистить лог</param>
    /// <exception cref="InvalidOperationException"></exception>
    private void MaybeTruncateUntil(Lsn index)
    {
        if (_tail.LastRecordIndex + 1 < index)
        {
            // Нужно удалить все сегменты и начать новый с указанного
            // Удаление производим с конца, т.к. в случае чего можно будет восстановить состояние с помощью снапшота
            _logger.Information("Удаляю сегмент {FileName}", _tail.File.FullName);
            _tail.Delete();
            for (var i = _sealed.Count - 1; i >= 0; i--)
            {
                var s = _sealed[i];
                _logger.Information("Удаляю сегмент {FileName}", s.File.FullName);
                s.Delete();
            }

            // Создаем новый файл сегмента (хвост) и обновляем состояние
            var newSegmentFileName = new SegmentFileName(index);
            var newSegmentFileInfo = GetSegmentFileInfo(_logDirectory, newSegmentFileName);
            _tail = LogSegment.InitializeTail(newSegmentFileInfo, newSegmentFileName, _options, _logger);
            _sealed.Clear();
            CommitIndex = index - 1;
            return;
        }

        if (_tail.LogStartIndex <= index)
        {
            // Просто перезапишем существующие записи - ничего удалять не надо
            return;
        }

        if (index < StartIndex)
        {
            // По хорошему это надо обновлять на уровне выше.
            // Но сейчас как будто уже все сделано - этих записей точно нет 
            return;
        }

        // Индекс указывает куда-то внутри лога, но не хвост - удаляем все сегменты до нужного и делаем его новым хвостом
        var newTail = _tail;

        var toDelete = new List<LogSegment>(1)
        {
            _tail, // В хвосте его точно нет
        };

        // Добавляем в обратном порядке, чтобы удалять в порядке прямого обхода
        for (var i = _sealed.Count - 1; i >= 0; i--)
        {
            var segment = _sealed[i];
            if (segment.Contains(index))
            {
                newTail = segment;
                break;
            }

            toDelete.Add(segment);
        }

        Debug.Assert(!ReferenceEquals(newTail, _tail), "!ReferenceEquals(newTail, _tail)",
            "Указанный индекс обязан содержаться среди одного из закрытых сегментов");

        // В списке toDelete сегменты находятся в обратном порядке - удаляем с конца:
        // Если возникнет ошибка, то останемся в корректном состоянии и сможем восстановиться из снапшота (хоть как-то)
        foreach (var segment in toDelete)
        {
            _logger.Information("Удаляю сегмент лога {LogSegment}", segment.FileName);
            segment.Delete();
        }

        // Обновляем состояние объекта лога
        newTail.MakeTail();
        _tail = newTail;
        var deleteCount = toDelete.Count - 1;
        var deleteStart = _sealed.Count - deleteCount;
        _sealed.RemoveRange(deleteStart, deleteCount);
    }

    /// <summary>
    /// Очистить весь лог и начать новый с указанного индекса
    /// </summary>
    /// <param name="startIndex">Новый индекс начала</param>
    /// <remarks>Используется во время инициализации, когда лог отстает от снапшота. В других случая не использовать</remarks>
    public void StartNewWith(Lsn startIndex)
    {
        // Так как этот метод вызывается, когда индекс снапшота больше последнего индекса в логе,
        // то мы можем спокойно удалять любой сегмент, т.к. нужно будет выполнить эту операцию в любом случае.
        // Т.е. будем повторять "до победного"
        foreach (var s in _sealed)
        {
            s.Delete();
        }

        _sealed.Clear();
        _tail.Delete();
        _tail = null!;

        var newTailName = new SegmentFileName(startIndex);
        var newTailFileInfo = GetSegmentFileInfo(_logDirectory, newTailName);

        var tail = LogSegment.InitializeTail(newTailFileInfo, newTailName, _options, _logger);
        _tail = tail;

        CommitIndex = startIndex - 1;
    }

    /// <summary>
    /// Добавить указанную запись в конец лога
    /// </summary>
    /// <param name="entry">Запись, которую нужно добавить в конец лога</param>
    /// <returns>Индекс добавленной записи</returns>
    public Lsn Append(LogEntry entry)
    {
        using var _ = BeginWriteLock();
        return _tail.Append(entry);
    }

    internal void SetupLogTest(IReadOnlyList<LogEntry> tailEntries)
    {
        _tail.SetupTest(tailEntries);
    }


    internal void SetupLogTest(IReadOnlyList<LogEntry> tailEntries,
        IReadOnlyList<IReadOnlyList<LogEntry>> sealedSegments,
        Lsn? startIndex = null,
        Lsn? commitIndex = null)
    {
        _tail.Delete();
        foreach (var s in _sealed)
        {
            s.Delete();
        }

        _sealed.Clear();

        var index = startIndex ?? 0;
        foreach (var data in sealedSegments)
        {
            Debug.Assert(data.Count > 0, "data.Count > 0", "Сегмент не может быть пуст");
            var fileName = new SegmentFileName(index);
            var fileInfo = GetSegmentFileInfo(_logDirectory, fileName);
            var segment = LogSegment.InitializeSealed(fileInfo, fileName, _options, _logger);
            segment.SetupTest(data);
            _sealed.Add(segment);
            index += segment.Count;
        }


        var tailFileName = new SegmentFileName(index);
        var tailFileInfo = GetSegmentFileInfo(_logDirectory, tailFileName);
        _tail = LogSegment.InitializeTail(tailFileInfo, tailFileName, _options, _logger);
        _tail.SetupTest(tailEntries);

        if (commitIndex is { } ci)
        {
            if (ci < StartIndex - 1)
            {
                throw new ArgumentOutOfRangeException(nameof(ci), ci,
                    "Индекс коммита не может быть меньше предначального индекса лога");
            }

            if (LastRecordIndex < ci)
            {
                throw new ArgumentOutOfRangeException(nameof(ci), ci,
                    "Индекс коммита не может быть больше последнего индекса лога");
            }

            CommitIndex = ci;
        }
        else
        {
            CommitIndex = StartIndex - 1;
        }
    }

    internal IReadOnlyList<LogEntry> ReadTailTest()
    {
        return _tail.ReadAll(long.MaxValue, out _);
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
                $"Индекс начала чтения не может быть больше индекса конца {end}");
        }

        using var _ = BeginReadLock();

        if (LastRecordIndex < end)
        {
            throw new ArgumentOutOfRangeException(nameof(end), end,
                "Указанный индекс конца диапазона выходит за пределы лога");
        }

        if (start < StartIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(start), start,
                "Указанный индекс начала диапазона выходит за пределы лога");
        }

        /*
         * Замечание: проверять ограничение на размер прочитанных данных здесь не нужно (пока)
         *
         * Этот метод используется для восстановления состояния приложения - чтение хранящихся команд в сегментах.
         * Во время чтения сегментов МОЖЕТ возникнуть ситуация когда весь 64+ Мб файл будет прочитан и будет висеть в памяти (до GC).
         * Этот вариант я пока допускаю, т.к. восстановление состояния будет происходить не так часто.
         * Поэтому ограничение на размер не проверяется, но в будущем (возможно) такую фичу добавлю.
         * Далее, все ограничения на размер прочитанных записей снимаются.
         */

        if (_tail.Contains(start))
        {
            foreach (var data in _tail.ReadRange(start, end, NoSizeCheck, out var _).Select(x => x.Data))
            {
                yield return data;
            }

            yield break;
        }

        // Вначале пройдемся по всем закрытым сегментам и прочитаем данные оттуда
        foreach (var segment in _sealed.Append(_tail))
        {
            if (segment.LastRecordIndex < start)
            {
                // Еще не дошли до нужного сегмента
                continue;
            }

            if (end < segment.LogStartIndex)
            {
                // Уже прошли нужный отрезок
                break;
            }

            /*
             * Теперь диапазон:
             * 1. Либо весь сегмент             (start <= segment.Start && segment.End <= end)
             * 2. Либо начало сегмента          (start <= segment.Start && end < segment.End)
             * 3. Либо последняя часть сегмента (segment.Start < start && segment.End <= end)
             * 4. Либо диапазон внутри сегмента (segment.Start < start && end < segment.End)
             *
             * Причем, если выполнены 2 или 4, то можно прекратить обработку дальнейших сегментов
             */
            IReadOnlyList<LogEntry> entries;
            var shouldStop = false;
            if (start <= segment.LogStartIndex)
            {
                if (segment.LastRecordIndex <= end)
                {
                    // Весь сегмент
                    entries = segment.ReadAll(NoSizeCheck, out var _);
                }
                else
                {
                    // Начало сегмента
                    entries = segment.ReadRange(segment.LogStartIndex, end, NoSizeCheck, out var _);
                    shouldStop = true;
                }
            }
            else
            {
                if (segment.LastRecordIndex <= end)
                {
                    // Последняя часть сегмента 
                    entries = segment.ReadRange(start, segment.LastRecordIndex, NoSizeCheck, out var _);
                }
                else
                {
                    // Внутренняя часть сегмента
                    entries = segment.ReadRange(start, end, NoSizeCheck, out var _);
                    shouldStop = true;
                }
            }

            foreach (var bytes in entries.Select(static e => e.Data))
            {
                yield return bytes;
            }

            if (shouldStop)
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
        return _sealed.Append(_tail)
            .Aggregate(new List<LogEntry>(), (entries, segment) =>
            {
                entries.AddRange(segment.ReadAll(long.MaxValue, out _));
                return entries;
            });
    }

    public bool TryGetLogEntryInfo(Lsn index, out LogEntryInfo entry)
    {
        using var _ = BeginReadLock();
        if (TryGetSegmentContaining(index, out var segment))
        {
            entry = segment.GetInfoAt(index);
            return true;
        }

        entry = LogEntryInfo.Tomb;
        return false;
    }

    /// <summary>
    /// Уведомить лог, что был закоммичен указанный индекс.
    /// При его вызове возможно будет создан новый файл сегмента, если размер текущего превысил предел
    /// </summary>
    /// <param name="commitIndex">Новый индекс коммита</param>
    public void Commit(Lsn commitIndex)
    {
        if (LastRecordIndex < commitIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(commitIndex), commitIndex,
                "Нельзя выставить индекс коммита больше последнего индекса в лога");
        }

        // Создавать новый сегмент 
        if (CommitIndex < commitIndex)
        {
            _logger.Information("Коммичу запись по индексу {CommitIndex}", commitIndex);
            CommitIndex = commitIndex;
        }
    }

    /// <summary>
    /// Создать новый файл сегмента, в случае если условие создания было выполнено.
    /// </summary>
    /// <remarks>
    /// Должен вызываться когда захвачена Write блокировка 
    /// </remarks>
    private void MaybeBeginNewSegment()
    {
        if (ShouldBeginNewSegment())
        {
            BeginNewSegment();
        }
    }

    private void BeginNewSegment()
    {
        Debug.Assert(_tail.Count > 0, "_tail.Count > 0", "Зачем создавать новый сегмент для пустого файла сегмента");

        // 1. Создаем новый файл сегмента со следующим индексом
        var nextSegmentFileName = new SegmentFileName(_tail.LastRecordIndex + 1);
        var nextSegmentFileInfo = GetSegmentFileInfo(_logDirectory, nextSegmentFileName);

        _logger.Information("Создаю новый файл сегмента {SegmentFileName}", nextSegmentFileInfo.FullName);
        var nextSegmentFile = LogSegment.InitializeTail(nextSegmentFileInfo, nextSegmentFileName, _options, _logger);

        // 2. Закрываем текущий хвост
        _tail.Seal();
        _sealed.Add(_tail);

        // 3. Обновляем хвост
        _tail = nextSegmentFile;
    }

    private bool ShouldBeginNewSegment()
    {
        /*
         * Новый сегмент создается если:
         * - Либо мягкий предел размера файла превышен и все записи закоммичены
         * - Либо жесткий предел размера файла превышен
         *
         * В идеале, это должно уменьшить количество удалений и созданий файлов сегментов,
         * когда перезаписываются незакоммиченные записи
         */

        var tailSize = _tail.GetEffectiveFileSize();
        return (_options.LogFileSoftLimit < tailSize && _tail.LastRecordIndex <= CommitIndex)
               || _options.LogFileHardLimit < tailSize;
    }

    internal void ValidateFileTest()
    {
        // throw new NotImplementedException();
    }

    private ReadLockScopeHandler BeginReadLock()
    {
        _lock.EnterReadLock();
        return new ReadLockScopeHandler(_lock);
    }

    private WriteLockScopeHandler BeginWriteLock()
    {
        _lock.EnterWriteLock();
        return new WriteLockScopeHandler(_lock);
    }

    private readonly struct ReadLockScopeHandler(ReaderWriterLockSlim l) : IDisposable
    {
        private readonly ReaderWriterLockSlim? _lock = l;

        public void Dispose()
        {
            try
            {
                _lock?.ExitReadLock();
            }
            catch (SynchronizationLockException)
            {
                Debug.Assert(false, "false", "Попытка освободить блокировку чтения, которая не держится");
            }
        }
    }

    private readonly struct WriteLockScopeHandler(ReaderWriterLockSlim l) : IDisposable
    {
        private readonly ReaderWriterLockSlim? _lock = l;

        public void Dispose()
        {
            try
            {
                _lock?.ExitWriteLock();
            }
            catch (SynchronizationLockException)
            {
                Debug.Assert(false, "false", "Попытка освободить блокировку чтения, которая не держится");
            }
        }
    }

    internal void SetCommitIndexTest(Lsn commit)
    {
        if (LastRecordIndex < commit)
        {
            throw new ArgumentOutOfRangeException(nameof(commit), commit,
                "Нельзя выставить индекс коммита больше, чем индекс последней записи в логе");
        }

        if (commit < (long)StartIndex - 1)
        {
            throw new ArgumentOutOfRangeException(nameof(commit), commit,
                "Нельзя выставить индекс коммита меньше, чем предпоследний в логе");
        }

        CommitIndex = commit;
    }

    /// <summary>
    /// Получить общее кол-во сегментов лога
    /// </summary>
    internal int GetSegmentsCount()
    {
        return _sealed.Count + 1;
    }

    /// <summary>
    /// Сгенерировать нужное кол-во записей сегмента, которые в файле сегмента будут занимать места не меньше чем указанное число байт.
    /// Используется весь размер лога, а не только самих записей. Т.е. маркеры, чек-суммы и т.д. тоже учитываются
    /// </summary>
    /// <param name="atLeastSizeBytes">Минимальный размер файла лога, который нужно сгенерировать</param>
    /// <param name="startTerm">Начальный терм записей. Будет постоянно увеличиваться для каждой записи</param>
    /// <returns>Записи, которые если записать в лог, то файл будет занимать как минимум <paramref name="atLeastSizeBytes"/> байт</returns>
    internal static IReadOnlyList<LogEntry> GenerateEntriesForSizeAtLeast(long atLeastSizeBytes, Term startTerm)
    {
        // Меньше этого
        long size = HeaderSizeBytes // Заголовок  
                    + sizeof(int); // Маркер конца

        var entries = new List<LogEntry>();
        var random = new Random();
        while (size < atLeastSizeBytes)
        {
            var data = new byte[random.Next(0, 1000)];
            random.NextBytes(data);
            var entry = new LogEntry(startTerm, data);
            size += entry.CalculateFileRecordSize();
            entries.Add(entry);
        }

        return entries;
    }

    /// <summary>
    /// Удалить все сегменты, покрываемые указанным индексом.
    /// Возможно:
    /// - Никакие удалены не будут
    /// - Удалены будут все закрытые сегменты
    /// - Удалены все сегменты и начат новый с указанным индексом
    /// </summary>
    /// <param name="index">Индекс, до которого нужно удалить записи</param>
    public void DeleteCoveredSegmentsUntil(Lsn index)
    {
        using var _ = BeginWriteLock();

        /*
         * Возможные ситуации:
         * - index < StartIndex     - ничего не делаем и уходим
         * - LastIndex < index      - удаляем все сегменты и начинаем новый
         * - Tail.Contains(index)   - удаляем все закрытые сегменты
         * - иначе                  - ищем первый сегмент, который содержит указанный индекс и удаляем все до него
         */

        if (index < StartIndex)
        {
            return;
        }

        if (LastRecordIndex < index)
        {
            DropAllAndStartNewLog(index);
            return;
        }

        if (_tail.Contains(index))
        {
            DeleteAllSealedSegments();
            return;
        }

        var toDelete = _sealed.TakeWhile(s => s.LastRecordIndex < index)
            .ToList();
        toDelete.ForEach(static s => s.Delete());
        _sealed.RemoveRange(0, toDelete.Count);
    }

    private void DeleteAllSealedSegments()
    {
        foreach (var s in _sealed)
        {
            s.Delete();
        }

        _sealed.Clear();
    }

    /// <summary>
    /// Удалить ВООБЩЕ все сегменты и начать новый лог, начиная с указанного индекса.
    /// Используется, когда нужно удалить все записи и начать работать с указанного индекса после создания/установки снапшота.
    /// </summary>
    /// <param name="startIndex">Новый индекс начала</param>
    /// <remarks>Должен быть вызван, когда все записи устарели и надо быстро "улететь" вперед по индексам.
    /// Вызывать, когда указанный индекс больше последнего</remarks>
    private void DropAllAndStartNewLog(Lsn startIndex)
    {
        /*
         * На всякий случай, сначала создадим новый файл сегмента,
         * и только после начнем удалять старые сегменты.
         */
        Debug.Assert(LastRecordIndex < startIndex, "LastIndex < startIndex",
            "Все удалять и начинать новый надо когда новый индекс больше всех предыдущих");
        var newSegmentFileName = new SegmentFileName(startIndex);
        var newSegmentFileInfo = GetSegmentFileInfo(_logDirectory, newSegmentFileName);

        // Вначале создадим файл сегмента и инициализируем его необходимыми данными
        _logger.Information("Удаляю все сегменты и начинаю новый по индексу {StartIndex}", startIndex);

        // Этот поток станет новым хвостом, поэтому закрывать его не надо
        var newTail = LogSegment.InitializeTail(newSegmentFileInfo, newSegmentFileName, _options, _logger);

        foreach (var s in _sealed)
        {
            _logger.Information("Удаляю сегмент {SegmentName}", s.File.FullName);
            s.Delete();
        }

        _logger.Information("Удаляю сегмент {SegmentName}", _tail.File.FullName);
        _tail.Delete();
        _sealed.Clear();

        _tail = newTail;
        CommitIndex = _tail.LogStartIndex - 1;
    }

    internal List<SegmentFileName> GetAllSegmentNamesTest()
    {
        var names = new List<SegmentFileName>(_sealed.Count + 1);
        names.AddRange(_sealed.Select(s => s.FileName));
        names.Add(_tail.FileName);
        return names;
    }

    /// <summary>
    /// Подсчитать размер всех сегментов лога в байтах.
    /// Используется для метрик
    /// </summary>
    /// <returns>Суммарный размер всех сегментов лога</returns>
    internal long CalculateLogSegmentsTotalSize()
    {
        using var _ = BeginReadLock();

        var resultSize = _sealed.Sum(static s => GetFileSizeSafe(s.File));
        resultSize += GetFileSizeSafe(_tail.File);
        return resultSize;

        static long GetFileSizeSafe(IFileInfo fi)
        {
            try
            {
                return fi.Length;
            }
            catch (IOException)
            {
                return 0;
            }
        }
    }
}