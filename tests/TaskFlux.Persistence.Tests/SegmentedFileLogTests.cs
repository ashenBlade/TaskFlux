using System.IO.Abstractions;
using System.Text;
using Serilog.Core;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Persistence.Log;
using Xunit;

namespace TaskFlux.Persistence.Tests;

[Trait("Category", "Persistence")]
public class SegmentedFileLogTests : IDisposable
{
    private static LogEntry Entry(int term, string data)
        => new(new Term(term), Encoding.UTF8.GetBytes(data));

    private static LogEntry Entry(Term term, string data)
        => new(term, Encoding.UTF8.GetBytes(data));

    private record MockFiles(IDirectoryInfo LogDirectory, IDirectoryInfo DataDirectory);

    /// <summary>
    /// Файлы, который создаются вызовом <see cref="CreateLog()"/>.
    /// Это поле используется для проверки того, что после операции файл остается в корректном состоянии.
    /// </summary>
    private MockFiles? _createdFiles;

    private SegmentedFileLog? _createdFileLog;

    public void Dispose()
    {
        if (_createdFiles is var (_, dataDir))
        {
            var ex = Record.Exception(() => SegmentedFileLog.Initialize(dataDir, Logger.None));
            Assert.Null(ex);
        }

        if (_createdFileLog is not null)
        {
            var ex = Record.Exception(() => _createdFileLog.ValidateFileTest());
            Assert.Null(ex);
        }
    }

    /// <summary>
    /// Создать и инициализировать сегментированный лог из пустого состояния
    /// </summary>
    private (SegmentedFileLog Log, MockFiles Mock) CreateLog()
    {
        var fileSystem = Helpers.CreateFileSystem();
        var mockFiles = new MockFiles(fileSystem.Log, fileSystem.DataDirectory);
        _createdFiles = mockFiles;
        var fileLog = SegmentedFileLog.Initialize(fileSystem.DataDirectory, Logger.None);
        _createdFileLog = fileLog;
        return ( fileLog, mockFiles );
    }

    private (SegmentedFileLog Log, MockFiles Mock) CreateLog(Lsn start,
                                                             IReadOnlyList<LogEntry>? tailEntries = null,
                                                             IReadOnlyList<IReadOnlyList<LogEntry>>? segmentEntries =
                                                                 null,
                                                             SegmentedFileLogOptions? options = null)
    {
        var fileSystem = Helpers.CreateFileSystem();
        var mockFiles = new MockFiles(fileSystem.Log, fileSystem.DataDirectory);
        _createdFiles = mockFiles;
        var fileLog = SegmentedFileLog.InitializeTest(fileSystem.Log, start, tailEntries, segmentEntries, GetOptions());
        _createdFileLog = fileLog;
        return ( fileLog, mockFiles );

        SegmentedFileLogOptions GetOptions()
        {
            return options ?? new SegmentedFileLogOptions(long.MaxValue, long.MaxValue);
        }
    }

    [Fact]
    public void ReadLog__КогдаЛогПуст__ДолженВернутьПустойСписок()
    {
        var (storage, _) = CreateLog();

        var log = storage.ReadAllTest();

        Assert.Empty(log);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    [InlineData(20)]
    public void ReadLog__КогдаЛогПустИОперацияПовториласьНесколькоРаз__ДолженВернутьПустойСписок(int operationsCount)
    {
        var (storage, _) = CreateLog();

        for (int i = 0; i < operationsCount; i++)
        {
            var log = storage.ReadAllTest();

            Assert.Empty(log);
        }
    }

    [Fact]
    public void ReadLogПослеAppend__КогдаЛогПуст__ДолженВернутьСписокИзЕдинственнойЗаписи()
    {
        var (storage, _) = CreateLog();

        var entry = Entry(1, "some data");
        storage.Append(entry);
        var log = storage.ReadAllTest();

        Assert.Single(log);
    }

    private static readonly LogEntryEqualityComparer Comparer = new();

    [Fact]
    public void ReadLogПослеAppend__КогдаЛогПуст__ДолженВернутьСписокИзТойЖеЗаписи()
    {
        var (storage, _) = CreateLog();

        var expected = Entry(1, "some data");
        storage.Append(expected);
        var actual = storage.ReadAllTest().Single();

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    [InlineData(20)]
    public void ReadLogПослеНесколькихAppend__КогдаЛогПуст__ДолженВернутьСписокСТакимЖеКоличествомДобавленныхЗаписей(
        int entriesCount)
    {
        var (storage, _) = CreateLog();

        for (int i = 1; i <= entriesCount; i++)
        {
            var expected = Entry(i, $"some data {i}");
            storage.Append(expected);
        }

        var actual = storage.ReadAllTest();
        Assert.Equal(entriesCount, actual.Count);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    [InlineData(20)]
    public void ReadLogПослеНесколькихAppend__КогдаЛогПуст__ДолженВернутьСписокСДобавленнымиЗаписями(int entriesCount)
    {
        var (storage, _) = CreateLog();

        var expected = Enumerable.Range(1, entriesCount)
                                 .Select(i => Entry(i, $"data{i}"))
                                 .ToArray();

        foreach (var entry in expected)
        {
            storage.Append(entry);
        }

        var actual = storage.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    [InlineData(20)]
    public void ReadLogПослеAppendRange__КогдаЛогПуст__ДолженВернутьСписокСТакимЖеКоличествомДобавленныхЗаписей(
        int entriesCount)
    {
        var (storage, _) = CreateLog();

        var expected = Enumerable.Range(1, entriesCount)
                                 .Select(i => Entry(i, $"data{i}"))
                                 .ToArray();

        storage.SetupLogTest(expected);

        var actual = storage.ReadAllTest();
        Assert.Equal(entriesCount, actual.Count);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(2, 2)]
    [InlineData(2, 5)]
    [InlineData(3, 5)]
    [InlineData(10, 5)]
    [InlineData(10, 10)]
    public void ReadLog__КогдаВЛогеЕстьЗаписиИОперацияПовторяетсяНесколькоРаз__ДолженВозвращатьТеЖеЗаписи(
        int readCount,
        int entriesCount)
    {
        var (storage, _) = CreateLog();

        var expected = Enumerable.Range(1, entriesCount)
                                 .Select(i => Entry(i, $"data{i}"))
                                 .ToArray();

        storage.SetupLogTest(expected);

        for (int i = 0; i < readCount; i++)
        {
            var actual = storage.ReadAllTest();
            Assert.Equal(expected, actual, Comparer);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(20)]
    public void ПриПередачеУжеЗаполненногоЛога__ДолженСчитатьСохраненныеЗаписи(int entriesCount)
    {
        using var memory = new MemoryStream();
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();

        var (firstLog, fs) = CreateLog();
        firstLog.SetupLogTest(entries);

        var secondLog = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None);
        var actual = secondLog.ReadAllTest();

        Assert.Equal(entries, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 2)]
    [InlineData(2, 1)]
    [InlineData(10, 5)]
    [InlineData(5, 10)]
    public void InsertRangeOverwrite__СНеПустымЛогомИИндексомСледующимПослеКонца__ДолженДобавитьЗаписиВКонец(
        int initialSize,
        int appendSize)
    {
        var initial = Enumerable.Range(1, initialSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var appended = Enumerable.Range(1 + initialSize, appendSize)
                                 .Select(i => Entry(i, $"data {i}"))
                                 .ToArray();
        var expected = initial.Concat(appended).ToArray();

        var (log, _) = CreateLog();
        log.SetupLogTest(initial);

        log.InsertRangeOverwrite(appended, log.LastIndex + 1);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 2)]
    [InlineData(1, 10)]
    [InlineData(2, 2)]
    [InlineData(2, 1)]
    [InlineData(10, 1)]
    [InlineData(10, 20)]
    public void InsertRangeOverwrite__КогдаИндекс0__ДолженЗаменитьСодержимоеПереданнымиЗаписями(
        int logSize,
        int insertCount)
    {
        var (log, _) = CreateLog();
        var initialLog = CreateEntries(1, logSize);
        var toInsert = CreateEntries(1, insertCount);
        log.SetupLogTest(initialLog);
        var expected = toInsert;

        log.InsertRangeOverwrite(toInsert, 0);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(10, 10, 0)]
    [InlineData(10, 10, 1)]
    [InlineData(10, 10, 5)]
    [InlineData(10, 10, 9)]
    [InlineData(10, 4, 9)]
    [InlineData(10, 1, 9)]
    [InlineData(5, 1, 2)]
    [InlineData(5, 1, 3)]
    [InlineData(5, 1, 4)]
    [InlineData(5, 5, 3)]
    public void InsertRangeOverwrite__КогдаВставкаВнутрьЛога__ДолженПерезаписатьЛогСУказанногоИндекса(
        int logSize,
        int insertCount,
        int insertIndex)
    {
        var (log, _) = CreateLog();
        var initialLog = CreateEntries(1, logSize);
        var toInsert = CreateEntries(1, insertCount);
        log.SetupLogTest(initialLog);
        var expected = initialLog.Take(insertIndex)
                                 .Concat(toInsert)
                                 .ToArray();

        log.InsertRangeOverwrite(toInsert, insertIndex);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(100)]
    public void Commit__КогдаПредыдущийИндексКоммитаTomb__ДолженОбновитьИндексКоммита(long newCommitIndex)
    {
        var (log, _) = CreateLog();
        log.SetupLogTest(Enumerable.Range(1, ( int ) ( newCommitIndex + 1 ))
                                   .Select(i => Entry(i, i.ToString()))
                                   .ToArray());

        log.Commit(newCommitIndex);

        Assert.Equal(newCommitIndex, ( long ) log.CommitIndex);
    }

    [Fact]
    public void CommitIndex__КогдаЛогПуст__ДолженВернутьTomb()
    {
        var (log, _) = CreateLog();

        var actualCommit = log.CommitIndex;

        Assert.Equal(Lsn.Tomb, actualCommit);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(1, 1)]
    [InlineData(2, 0)]
    [InlineData(2, 1)]
    [InlineData(2, 100)]
    [InlineData(10, 10)]
    public void Append__КогдаВЛогеНесколькоСегментов__ДолженДобавитьЗаписьВПоследнийСегмент(
        int sealedSegmentsCount,
        int tailSize)
    {
        const int segmentSize = 1024;
        var term = new Term(1);
        var sealedSegmentEntries = Enumerable.Range(0, sealedSegmentsCount)
                                             .Select(i => Enumerable.Range(1, segmentSize)
                                                                    .Select(j => Entry(( int ) term,
                                                                         ( j + i * segmentSize ).ToString()))
                                                                    .ToArray())
                                             .ToArray();
        var tailEntries = Enumerable.Range(1, tailSize)
                                    .Select(i => Entry(( int ) term,
                                         ( sealedSegmentsCount * segmentSize + i ).ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(start: 0, tailEntries: tailEntries, segmentEntries: sealedSegmentEntries);
        var entry = new LogEntry(sealedSegmentsCount * segmentSize, "sample data"u8.ToArray());
        var expected = tailEntries.Append(entry).ToArray();

        log.Append(entry);

        var actual = log.ReadTailTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(1, 1)]
    [InlineData(2, 0)]
    [InlineData(2, 1)]
    [InlineData(2, 100)]
    [InlineData(10, 10)]
    public void Append__КогдаВЛогеНесколькоСегментов__ДолженВернутьКорректныйИндекс(
        int sealedSegmentsCount,
        int tailSize)
    {
        const int segmentSize = 1024;
        var term = new Term(1);
        var sealedSegmentEntries = Enumerable.Range(0, sealedSegmentsCount)
                                             .Select(i => Enumerable.Range(1, segmentSize)
                                                                    .Select(j => Entry(( int ) term,
                                                                         ( j + i * segmentSize ).ToString()))
                                                                    .ToArray())
                                             .ToArray();
        var tailEntries = Enumerable.Range(1, tailSize)
                                    .Select(i => Entry(( int ) term,
                                         ( sealedSegmentsCount * segmentSize + i ).ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(start: 0, tailEntries: tailEntries, segmentEntries: sealedSegmentEntries);
        var entry = new LogEntry(sealedSegmentsCount * segmentSize, "sample data"u8.ToArray());
        var expected = tailEntries.Append(entry).ToArray();

        log.Append(entry);

        var actual = log.ReadTailTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 10)]
    [InlineData(2, 1)]
    [InlineData(2, 2)]
    [InlineData(10, 50)]
    public void
        InsertRangeOverwrite__КогдаВЛогеНесколькоСегментовИИндексРавенПервомуВХвостеИХвостПуст__ДолженДобавитьВсеЗаписиВХвост(
        int sealedSegmentsCount,
        int count)
    {
        const int segmentSize = 1024;
        var term = new Term(1);
        var sealedSegmentEntries = Enumerable.Range(0, sealedSegmentsCount)
                                             .Select(i => Enumerable.Range(1, segmentSize)
                                                                    .Select(j => Entry(( int ) term,
                                                                         ( j + i * segmentSize ).ToString()))
                                                                    .ToArray())
                                             .ToArray();
        var (log, _) = CreateLog(start: 0, tailEntries: null, segmentEntries: sealedSegmentEntries);
        var toInsert = Enumerable.Range(0, count).Select(i => Entry(term, i.ToString())).ToArray();
        var expected = toInsert;

        log.InsertRangeOverwrite(toInsert, log.LastIndex + 1);

        var actual = log.ReadTailTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    // Размер уменьшается
    [InlineData(10, 1, 5)]
    [InlineData(10, 2, 6)]
    [InlineData(10, 3, 3)]
    [InlineData(20, 10, 13)]
    // Размер не меняется
    [InlineData(3, 3, 0)]
    [InlineData(10, 6, 4)]
    [InlineData(10, 5, 5)]
    [InlineData(10, 9, 1)]
    // Размер увеличивается
    [InlineData(10, 9, 2)]
    [InlineData(10, 2, 9)]
    [InlineData(10, 6, 5)]
    [InlineData(10, 10, 5)]
    public void InsertRangeOverwrite__КогдаВЛогеНесколькоСегментовИИндексВнутриХвоста__ДолженДобавитьВсеЗаписиВХвост(
        int tailInitialSize,
        int toInsertCount,
        int insertIndex)
    {
        // Взял из головы
        const int segmentSize = 1024;
        const int sealedSegmentsCount = 9;

        var sealedSegmentEntries = Enumerable.Range(0, sealedSegmentsCount)
                                             .Select(i => Enumerable.Range(1, segmentSize)
                                                                    .Select(j => Entry(j + i * segmentSize,
                                                                         ( j + i * segmentSize ).ToString()))
                                                                    .ToArray())
                                             .ToArray();
        var tailEntries = Enumerable.Range(sealedSegmentsCount * segmentSize, tailInitialSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(start: 0, tailEntries: tailEntries, segmentEntries: sealedSegmentEntries);
        var toInsert = Enumerable.Range(sealedSegmentsCount * segmentSize + tailInitialSize, toInsertCount)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        var expected = tailEntries.Take(insertIndex)
                                  .Concat(toInsert)
                                  .ToArray();
        var logInsertIndex = segmentSize * sealedSegmentsCount + insertIndex;

        log.InsertRangeOverwrite(toInsert, logInsertIndex);

        var actual = log.ReadTailTest();

        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void InsertRangeOverwrite__КогдаПревышенМягкийПределИВсеЗаписиБылиЗакоммичены__ДолженНачатьНовыйСегмент()
    {
        const long softLimit = 1024;       // 1Кб
        const long hardLimit = 1024 * 100; // Чтобы случайно не помешал
        var options = new SegmentedFileLogOptions(softLimit, hardLimit);
        // На всякий случай, сделаем побольше чуть-чуть
        var atLeastSizeBytes = softLimit + 10;
        var initialTailEntries = SegmentedFileLog.GenerateEntriesForSizeAtLeast(atLeastSizeBytes, 1);
        var (log, _) = CreateLog(0, initialTailEntries, options: options);
        var toInsert = Enumerable.Range(( int ) initialTailEntries[^1].Term.Value + 1, 10)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        log.SetCommitIndexTest(log.LastIndex);

        log.InsertRangeOverwrite(toInsert, log.LastIndex + 1);

        Assert.Equal(2, log.GetSegmentsCountTest());
        Assert.Equal(toInsert, log.ReadTailTest());
    }

    [Fact]
    public void InsertRangeOverwrite__КогдаПревышенЖесткийПредел__ДолженНачатьНовыйСегмент()
    {
        const long softLimit = 1024;     // 1 Кб
        const long hardLimit = 1024 * 2; // 2 Кб
        var options = new SegmentedFileLogOptions(softLimit, hardLimit);
        var initialTailEntries = SegmentedFileLog.GenerateEntriesForSizeAtLeast(hardLimit, 1);
        var (log, _) = CreateLog(0, initialTailEntries, options: options);
        var toInsert = Enumerable.Range(( int ) initialTailEntries[^1].Term.Value + 1, 10)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        // Как будто ничего не закоммичено
        // log.SetCommitIndexTest(log.LastIndex);

        log.InsertRangeOverwrite(toInsert, log.LastIndex + 1);

        Assert.Equal(2, log.GetSegmentsCountTest());
        Assert.Equal(toInsert, log.ReadTailTest());
    }

    [Theory]
    // Начало сегмента
    [InlineData(1, 0)]
    [InlineData(100, 0)]
    [InlineData(1024, 0)] // Полностью перетираем предыдущий сегмент
    // Внутри сегмента
    [InlineData(1, 10)]
    [InlineData(1, 512)]
    [InlineData(10, 100)]
    [InlineData(12, 62)]
    [InlineData(100, 512)]
    // Конец предыдущего сегмента
    [InlineData(1, 1023)]
    [InlineData(20, 1004)]
    public void
        InsertRangeOverwrite__КогдаСегментовНесколькоИИндексНаходитсяВПоследнемЗакрытомСегменте__ДолженСделатьЗаписиВПредыдущийСегмент(
        int insertCount,
        int prevSegmentInsertIndex)
    {
        const int sealedSegmentsCount = 10;
        const int segmentSize = 1024;
        const int tailSize = 128;
        var term = new Term(1);
        var sealedSegmentEntries = Enumerable.Range(0, sealedSegmentsCount)
                                             .Select(i => Enumerable.Range(1, segmentSize)
                                                                    .Select(j => Entry(( int ) term,
                                                                         ( j + i * segmentSize ).ToString()))
                                                                    .ToArray())
                                             .ToArray();
        var tailEntries = Enumerable.Range(1, tailSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(start: 0, tailEntries: tailEntries, segmentEntries: sealedSegmentEntries);
        var toInsert = Enumerable.Range(tailSize, insertCount)
                                 .Select(i => Entry(term, i.ToString()))
                                 .ToArray();
        var expected = sealedSegmentEntries[^1]
                      .Take(prevSegmentInsertIndex) // Записи из пред последнего закрытого сегмента до нужного индекса
                      .Concat(toInsert)             // И все вставляемые записи
                      .ToArray();
        var insertIndex =
            ( long ) segmentSize
          * ( sealedSegmentsCount - 1 ) // Последний индекс в пред пред последнем закрытом сегменте 
          + prevSegmentInsertIndex;

        log.InsertRangeOverwrite(toInsert, insertIndex);

        var actual = log.ReadTailTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    // Начало сегмента
    [InlineData(1, 0)]
    [InlineData(100, 0)]
    [InlineData(1024, 0)] // Полностью перетираем предыдущий сегмент
    // Внутри сегмента
    [InlineData(1, 10)]
    [InlineData(1, 512)]
    [InlineData(10, 100)]
    [InlineData(12, 62)]
    [InlineData(100, 512)]
    // Конец предыдущего сегмента
    [InlineData(1, 1023)]
    [InlineData(20, 1004)]
    public void
        InsertRangeOverwrite__КогдаСегментовНесколькоИИндексНаходитсяВПервомЗакрытомСегменте__ДолженСделатьЗаписиВПредыдущийСегмент(
        int insertCount,
        int segmentInsertIndex)
    {
        const int sealedSegmentsCount = 10;
        const int segmentSize = 1024;
        const int tailSize = 128;
        var term = new Term(1);
        var sealedSegmentEntries = Enumerable.Range(0, sealedSegmentsCount)
                                             .Select(i => Enumerable.Range(1, segmentSize)
                                                                    .Select(j => Entry(( int ) term,
                                                                         ( j + i * segmentSize ).ToString()))
                                                                    .ToArray())
                                             .ToArray();
        var tailEntries = Enumerable.Range(1, tailSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(start: 0, tailEntries: tailEntries, segmentEntries: sealedSegmentEntries);
        var toInsert = Enumerable.Range(tailSize, insertCount)
                                 .Select(i => Entry(term, i.ToString()))
                                 .ToArray();
        var expected = sealedSegmentEntries[0]
                      .Take(segmentInsertIndex) // Записи из пред последнего закрытого сегмента до нужного индекса
                      .Concat(toInsert)         // И все вставляемые записи
                      .ToArray();
        var insertIndex = segmentInsertIndex;

        log.InsertRangeOverwrite(toInsert, insertIndex);

        var actual = log.ReadTailTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    // Полностью пустой лог
    [InlineData(0, 0, 1, 1)]
    [InlineData(0, 0, 100, 1)]
    [InlineData(0, 0, 1, 100)]
    [InlineData(0, 0, 100, 100)]
    [InlineData(0, 0, 1, 123123123123)]
    [InlineData(0, 0, 50, 123123123123)]
    // Есть какие-то записи в хвосте
    [InlineData(0, 123, 1, 1)]
    [InlineData(0, 111, 50, 1)]
    [InlineData(0, 111, 1, 1654575)]
    [InlineData(0, 111, 50, 1654575)]
    // Есть уже закрытые сегменты
    [InlineData(1, 123, 1, 1)]
    [InlineData(1, 123, 1, 50)]
    [InlineData(1, 123, 50, 50)]
    [InlineData(10, 0, 111, 956784234)]
    [InlineData(10, 100, 111, 956784234)]
    [InlineData(1000, 233, 100, 3457647)]
    public void InsertRangeOverwrite__КогдаИндексБольшеПоследнего__ДолженУдалитьВсеПредыдущиеЗаписи(
        int sealedSegmentsCount,
        int tailSize,
        int insertCount,
        long indexGap)
    {
        const int segmentSize = 1024;
        var term = new Term(1);
        var sealedSegmentEntries = Enumerable.Range(0, sealedSegmentsCount)
                                             .Select(i => Enumerable.Range(1, segmentSize)
                                                                    .Select(j => Entry(( int ) term,
                                                                         ( j + i * segmentSize ).ToString()))
                                                                    .ToArray())
                                             .ToArray();
        var tailEntries = Enumerable.Range(1, tailSize).Select(i => Entry(term, i.ToString())).ToArray();
        var (log, _) = CreateLog(start: 0, tailEntries: tailEntries, segmentEntries: sealedSegmentEntries);
        var toInsert = Enumerable.Range(tailSize, insertCount).Select(i => Entry(term, i.ToString())).ToArray();
        var expected = toInsert;
        var insertIndex = log.LastIndex + indexGap + 1;

        log.InsertRangeOverwrite(toInsert, insertIndex);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void InsertRangeOverwrite__КогдаИндексБольшеПоследнего__ДолженУдалитьПредыдущиеФайлыСегментов()
    {
        const int segmentSize = 1024;
        const int sealedSegmentsCount = 10;
        const int tailSize = 100;
        const int insertCount = 100;
        const int indexGap = 1000;

        var term = new Term(1);
        var sealedSegmentEntries = Enumerable.Range(0, sealedSegmentsCount)
                                             .Select(i => Enumerable.Range(1, segmentSize)
                                                                    .Select(j => Entry(( int ) term,
                                                                         ( j + i * segmentSize ).ToString()))
                                                                    .ToArray())
                                             .ToArray();
        var tailEntries = Enumerable.Range(1, tailSize).Select(i => Entry(term, i.ToString())).ToArray();
        var (log, fs) = CreateLog(start: 0, tailEntries: tailEntries, segmentEntries: sealedSegmentEntries);
        var toInsert = Enumerable.Range(tailSize, insertCount).Select(i => Entry(term, i.ToString())).ToArray();
        var insertIndex = log.LastIndex + indexGap + 1;

        log.InsertRangeOverwrite(toInsert, insertIndex);

        var segmentsCount = log.GetSegmentsCountTest();
        Assert.Equal(1, segmentsCount);
        fs.LogDirectory.Refresh();
        var segmentFiles = fs.LogDirectory.GetFiles();
        Assert.Single(segmentFiles);
    }

    [Fact]
    public void InsertRangeOverwrite__КогдаИндексБольшеПоследнего__ДолженОбновитьStartIndex()
    {
        const int segmentSize = 1024;
        const int sealedSegmentsCount = 10;
        const int tailSize = 100;
        const int insertCount = 100;
        const int indexGap = 1000;

        var term = new Term(1);
        var sealedSegmentEntries = Enumerable.Range(0, sealedSegmentsCount)
                                             .Select(i => Enumerable.Range(1, segmentSize)
                                                                    .Select(j => Entry(( int ) term,
                                                                         ( j + i * segmentSize ).ToString()))
                                                                    .ToArray())
                                             .ToArray();
        var tailEntries = Enumerable.Range(1, tailSize).Select(i => Entry(term, i.ToString())).ToArray();
        var (log, _) = CreateLog(start: 0, tailEntries: tailEntries, segmentEntries: sealedSegmentEntries);
        var toInsert = Enumerable.Range(tailSize, insertCount).Select(i => Entry(term, i.ToString())).ToArray();
        var insertIndex = log.LastIndex + indexGap + 1;
        var expected = insertIndex;

        log.InsertRangeOverwrite(toInsert, insertIndex);

        Assert.Equal(expected, log.StartIndex);
    }

    private static List<LogEntry> CreateEntries(int startTerm, int count)
    {
        return Enumerable.Range(startTerm, count)
                         .Select(i => Entry(i, i.ToString()))
                         .ToList();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(20)]
    public void GetInfoAt__КогдаЛогНеПустойИндексВалидный__ДолженВернутьТребуемоеЗначение(int logSize)
    {
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();

        var (log, _) = CreateLog();
        log.SetupLogTest(initial);

        for (int index = 0; index < logSize; index++)
        {
            var expected = new LogEntryInfo(initial[index].Term, index);
            var actual = log.GetEntryInfoAt(index);
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public void GetLastLogEntry__СПустымЛогом__ДолженВернутьTomb()
    {
        var expected = LogEntryInfo.Tomb;

        var (log, _) = CreateLog();

        var actual = log.GetLastLogEntry();
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(20)]
    public void GetLastLogEntry__СЕдинственнымСегментом__ДолженВернутьПоследнююЗапись(int logSize)
    {
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();

        var (log, _) = CreateLog();
        log.SetupLogTest(initial);

        var expected = new LogEntryInfo(initial[^1].Term, initial.Length - 1);
        var actual = log.GetLastLogEntry();
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(4, 1)]
    [InlineData(4, 4)]
    [InlineData(10, 10)]
    public void InsertRangeOverwrite__КогдаИндексРавенСледующемуПослеПоследнего__ДолженЗаписатьДанныеВКонец(
        int initialCount,
        int toAppendCount)
    {
        var (log, _) = CreateLog();
        var initial = Enumerable.Range(1, initialCount)
                                .Select(i => Entry(i, i.ToString()))
                                .ToArray();
        log.SetupLogTest(initial);
        var toAppend = Enumerable.Range(initialCount + 1, toAppendCount)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        var expected = initial.Concat(toAppend).ToArray();
        var indexToInsert = initial.Length;

        log.InsertRangeOverwrite(toAppend, indexToInsert);

        Assert.Equal(expected, log.ReadAllTest(), Comparer);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(10, 1)]
    [InlineData(5, 10)]
    [InlineData(8, 2)]
    public void InsertRangeOverwrite__КогдаИндексРавенПоследнему__ДолженПерезаписатьПоследнююЗапись(
        int initialCount,
        int toAppendCount)
    {
        var (log, _) = CreateLog();
        var initial = Enumerable.Range(1, initialCount)
                                .Select(i => Entry(i, i.ToString()))
                                .ToArray();
        log.SetupLogTest(initial);

        var toAppend = Enumerable.Range(initialCount + 1, toAppendCount)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        var expected = initial.Take(initialCount - 1).Concat(toAppend).ToArray();
        var indexToInsert = initial.Length - 1;

        log.InsertRangeOverwrite(toAppend, indexToInsert);

        Assert.Equal(expected, log.ReadAllTest(), Comparer);
    }

    [Theory]
    [InlineData(5, 2, 1)]
    [InlineData(5, 2, 2)]
    [InlineData(4, 1, 2)]
    [InlineData(10, 3, 6)]
    public void InsertRangeOverwrite__КогдаИндексНаходитсяВЛоге__ДолженПерезаписатьСодержимоеЛога(
        int initialCount,
        int toAppendCount,
        int insertIndex)
    {
        var (log, _) = CreateLog();
        var initial = Enumerable.Range(1, initialCount)
                                .Select(i => Entry(i, i.ToString()))
                                .ToArray();
        log.SetupLogTest(initial);
        var toAppend = Enumerable.Range(initialCount + 1, toAppendCount)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        var expected = initial.Take(insertIndex)
                              .Concat(toAppend)
                              .ToArray();

        log.InsertRangeOverwrite(toAppend, insertIndex);

        Assert.Equal(expected, log.ReadAllTest(), Comparer);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 10)]
    [InlineData(5, 1)]
    [InlineData(5, 2)]
    [InlineData(5, 5)]
    [InlineData(5, 10)]
    public void InsertRangeOverwrite__КогдаИндексРавен0__ДолженПерезаписатьЛог(int initialCount, int toAppendCount)
    {
        var (log, _) = CreateLog();
        var initial = Enumerable.Range(1, initialCount)
                                .Select(i => Entry(i, i.ToString()))
                                .ToArray();
        log.SetupLogTest(initial);
        var toAppend = Enumerable.Range(initialCount + 1, toAppendCount)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        var expected = toAppend;

        log.InsertRangeOverwrite(toAppend, 0);

        Assert.Equal(expected, log.ReadAllTest(), Comparer);
    }

    [Fact]
    public void ReadDataRange__КогдаВЛоге1Запись__ДолженВернутьМассивСЭтойЗаписью()
    {
        var (log, _) = CreateLog();
        var entry = Entry(1, "headsf");
        log.SetupLogTest(new[] {entry});

        var actual = log.ReadDataRange(0, 0);

        Assert.Single(actual, d => d.SequenceEqual(entry.Data));
    }

    [Theory]
    [InlineData(10, 0, 9)]
    [InlineData(10, 0, 1)]
    [InlineData(10, 1, 8)]
    [InlineData(10, 5, 9)]
    [InlineData(100, 50, 99)]
    [InlineData(100, 50, 60)]
    public void ReadDataRange__КогдаДиапазонСодержитНесколькоЗаписей__ДолженВернутьУказанныеЗаписи(
        int logSize,
        int start,
        int end)
    {
        var (log, _) = CreateLog();
        var entries = Enumerable.Range(0, logSize).Select(i => Entry(1, i.ToString())).ToList();
        log.SetupLogTest(entries);
        var expected = entries.GetRange(start, end - start + 1);

        var actual = log.ReadDataRange(start, end).ToList();

        Assert.Equal(expected.Select(e => e.Data), actual, ByteComparer);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(49)]
    [InlineData(50)]
    public void ReadDataRange__КогдаНачалоИКонецРавныГраницамЛога__ДолженВернутьВсеСодержимоеЛога(int logSize)
    {
        var (log, _) = CreateLog();
        var entries = Enumerable.Range(0, logSize).Select(i => Entry(1, i.ToString())).ToList();
        log.SetupLogTest(entries);
        var expected = entries;

        var actual = log.ReadDataRange(0, log.LastIndex).ToList();

        Assert.Equal(expected.Select(e => e.Data), actual, ByteComparer);
    }

    private static readonly IEqualityComparer<byte[]> ByteComparer = new ByteArrayEqualityComparer();

    private class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[]? x, byte[]? y)
        {
            return x!.SequenceEqual(y!);
        }

        public int GetHashCode(byte[] obj)
        {
            return obj.Sum(b => b);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    public void
        DeleteUntil__КогдаВЛогеЕстьНесколькоЗакрытыхСегментовИИндексРавенНачальномуВХвосте__ДолженУдалитьВсеЗакрытыеСегменты(
        int sealedSegmentsCount)
    {
        const int segmentSize = 1024;
        const int tailSize = 10;
        var sealedSegmentsEntries = Enumerable.Range(0, sealedSegmentsCount)
                                              .Select(s => Enumerable.Range(0, segmentSize)
                                                                     .Select(i => Entry(1,
                                                                          ( i + s * segmentSize ).ToString()))
                                                                     .ToArray())
                                              .ToArray();
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (facade, _) = CreateLog(0, tailEntries: tailEntries, segmentEntries: sealedSegmentsEntries);
        var startSegmentIndex = segmentSize * sealedSegmentsCount;

        facade.DeleteUntil(startSegmentIndex);

        Assert.Equal(1, facade.GetSegmentsCountTest());
    }

    [Fact]
    public void DeleteUntil__КогдаИндексУказываетНаСегментВСередине__ДолженУдалитьСегментыДоЭтогоСегмента()
    {
        const int segmentSize = 1024;
        const int sealedSegmentsCount = 10;
        const int tailSize = 10;
        var sealedSegmentsEntries = Enumerable.Range(0, sealedSegmentsCount)
                                              .Select(s => Enumerable.Range(0, segmentSize)
                                                                     .Select(i => Entry(1,
                                                                          ( i + s * segmentSize ).ToString()))
                                                                     .ToArray())
                                              .ToArray();
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (facade, _) = CreateLog(0, tailEntries: tailEntries, segmentEntries: sealedSegmentsEntries);
        var startSegmentIndex = segmentSize * 4 + 1; // Индекс внутри 5-ого сегмента 
        var expectedSegmentsCount = 6 + 1;           // Удалили 4 сегмента (10 - 4) и хвост  

        facade.DeleteUntil(startSegmentIndex);

        Assert.Equal(expectedSegmentsCount, facade.GetSegmentsCountTest());
    }

    [Fact]
    public void DeleteUntil__КогдаИндексРавенНачальному__НеДолженУдалятьСегменты()
    {
        const int segmentSize = 1024;
        const int sealedSegmentsCount = 10;
        const int tailSize = 10;
        var sealedSegmentsEntries = Enumerable.Range(0, sealedSegmentsCount)
                                              .Select(s => Enumerable.Range(0, segmentSize)
                                                                     .Select(i => Entry(1,
                                                                          ( i + s * segmentSize ).ToString()))
                                                                     .ToArray())
                                              .ToArray();
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (facade, _) = CreateLog(0, tailEntries: tailEntries, segmentEntries: sealedSegmentsEntries);
        var startSegmentIndex = 0;
        var expectedSegmentsCount = sealedSegmentsCount + 1; // Все сегменты + хвост   

        facade.DeleteUntil(startSegmentIndex);

        Assert.Equal(expectedSegmentsCount, facade.GetSegmentsCountTest());
    }

    [Fact]
    public void DeleteUntil__КогдаБылиУдаленыСегменты__ДолженОбновитьНачальныйИндекс()
    {
        const int segmentSize = 1024;
        const int sealedSegmentsCount = 10;
        const int tailSize = 10;
        var sealedSegmentsEntries = Enumerable.Range(0, sealedSegmentsCount)
                                              .Select(s => Enumerable.Range(0, segmentSize)
                                                                     .Select(i => Entry(1,
                                                                          ( i + s * segmentSize ).ToString()))
                                                                     .ToArray())
                                              .ToArray();
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (facade, _) = CreateLog(0, tailEntries: tailEntries, segmentEntries: sealedSegmentsEntries);
        var startSegmentIndex = segmentSize * 5 + segmentSize / 2; // Указывает на середину 6 сегмента     
        var expectedStartIndex = segmentSize * 5;

        facade.DeleteUntil(startSegmentIndex);

        Assert.Equal(expectedStartIndex, facade.StartIndex);
    }

    [Fact]
    public void DeleteUntil__КогдаИндексБольшеПоследнего__ДолженУдалитьВсеСегменты()
    {
        const int segmentSize = 1024;
        const int sealedSegmentsCount = 10;
        const int tailSize = 10;
        const int deleteIndex = segmentSize * sealedSegmentsCount + tailSize + 1;
        var sealedSegmentsEntries = Enumerable.Range(0, sealedSegmentsCount)
                                              .Select(s => Enumerable.Range(0, segmentSize)
                                                                     .Select(i => Entry(1,
                                                                          ( i + s * segmentSize ).ToString()))
                                                                     .ToArray())
                                              .ToArray();
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (facade, _) = CreateLog(0, tailEntries: tailEntries, segmentEntries: sealedSegmentsEntries);

        facade.DeleteUntil(deleteIndex);

        Assert.Equal(1, facade.GetSegmentsCountTest());
        Assert.Empty(facade.ReadAllTest());
    }

    [Fact]
    public void DeleteUntil__КогдаИндексБольшеПоследнего__ДолженОбновитьНачальныйИндекс()
    {
        const int segmentSize = 1024;
        const int sealedSegmentsCount = 10;
        const int tailSize = 10;
        const int deleteIndex = segmentSize * sealedSegmentsCount + tailSize + 1;
        var sealedSegmentsEntries = Enumerable.Range(0, sealedSegmentsCount)
                                              .Select(s => Enumerable.Range(0, segmentSize)
                                                                     .Select(i => Entry(1,
                                                                          ( i + s * segmentSize ).ToString()))
                                                                     .ToArray())
                                              .ToArray();
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (facade, _) = CreateLog(0, tailEntries: tailEntries, segmentEntries: sealedSegmentsEntries);

        facade.DeleteUntil(deleteIndex);

        Assert.Equal(deleteIndex, facade.StartIndex);
    }

    [Fact]
    public void DeleteUntil__КогдаИндексУказываетНаЗаписьВнутриПервогоЗакрытогоСегмента__НеДолженУдалятьСегменты()
    {
        const int segmentSize = 1024;
        const int sealedSegmentsCount = 10;
        const int tailSize = 10;
        var sealedSegmentsEntries = Enumerable.Range(0, sealedSegmentsCount)
                                              .Select(s => Enumerable.Range(0, segmentSize)
                                                                     .Select(i => Entry(1,
                                                                          ( i + s * segmentSize ).ToString()))
                                                                     .ToArray())
                                              .ToArray();
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (facade, _) = CreateLog(0, tailEntries: tailEntries, segmentEntries: sealedSegmentsEntries);
        var expectedSegmentsCount = sealedSegmentsCount + 1;
        var deleteIndex = segmentSize / 2;

        facade.DeleteUntil(deleteIndex);

        Assert.Equal(expectedSegmentsCount, facade.GetSegmentsCountTest());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    public void TryGetFrom__КогдаУказанныйИндексРавенПервомуВХвостеИЕстьЗакрытые__ДолженВернутьЗаписиИзХвоста(
        int tailSize)
    {
        var term = 1;
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var segmentsCount = 4;
        var segmentSize = 128;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e =>
                                                                   Entry(term, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var (log, _) = CreateLog(0, tailEntries, sealedSegments);
        var index = segmentSize * segmentsCount;
        var expected = tailEntries;

        _ = log.TryGetFrom(index, out var actual, out _);

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    public void
        TryGetFrom__КогдаУказанныйИндексРавенПервомуВХвостеИЕстьЗакрытые__ДолженВернутьДанныеОПоследнейЗаписиВПоследнемСегменте(
        int tailSize)
    {
        var term = 1;
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var segmentsCount = 4;
        var segmentSize = 128;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e =>
                                                                   Entry(term, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var index = segmentSize * segmentsCount;
        var expected = new LogEntryInfo(sealedSegments[^1][^1].Term, index - 1);
        var (log, _) = CreateLog(0, tailEntries, sealedSegments);


        _ = log.TryGetFrom(index, out _, out var actual);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    public void TryGetFrom__КогдаУказанныйИндексРавенПервомуВХвостеИЕстьЗакрытые__ДолженВернутьTrue(
        int tailSize)
    {
        var term = 1;
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var segmentsCount = 4;
        var segmentSize = 128;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e =>
                                                                   Entry(term, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var (log, _) = CreateLog(0, tailEntries, sealedSegments);
        var index = segmentSize * segmentsCount;

        var success = log.TryGetFrom(index, out _, out _);

        Assert.True(success);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    public void
        TryGetFrom__КогдаУказанныйИндексРавенНачальномуВХвостеИЕстьЗакрытыеСегментыИХвостПуст__ДолженВернутьTrue(
        int segmentsCount)
    {
        var term = 1;
        var segmentSize = 128;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e =>
                                                                   Entry(term, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var (log, _) = CreateLog(0, Array.Empty<LogEntry>(), sealedSegments);
        var index = segmentSize * segmentsCount;

        var success = log.TryGetFrom(index, out _, out _);

        Assert.True(success);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    public void
        TryGetFrom__КогдаУказанныйИндексРавенНачальномуВХвостеИЕстьЗакрытыеСегментыИХвостПуст__ДолженВернутьПустойМассив(
        int segmentsCount)
    {
        var term = 1;
        var segmentSize = 128;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e =>
                                                                   Entry(term, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var (log, _) = CreateLog(0, Array.Empty<LogEntry>(), sealedSegments);
        var index = segmentSize * segmentsCount;

        _ = log.TryGetFrom(index, out var actual, out _);

        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    public void
        TryGetFrom__КогдаУказанныйИндексРавенНачальномуВХвостеИЕстьЗакрытыеСегментыИХвостПуст__ДолженВернутьДанныеОЗаписиВПоследнемСегменте(
        int segmentsCount)
    {
        var term = 1;
        var segmentSize = 128;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e =>
                                                                   Entry(term, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var (log, _) = CreateLog(0, Array.Empty<LogEntry>(), sealedSegments);
        var index = segmentSize * segmentsCount;
        var expected = new LogEntryInfo(term, index - 1);

        _ = log.TryGetFrom(index, out _, out var actual);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void TryGetFrom__КогдаЛогПустИИндексРавен0ИЛогНачинаетсяСИндекса0__ДолженВернутьПустойМассив()
    {
        var (facade, _) = CreateLog(0);

        _ = facade.TryGetFrom(0, out var actual, out _);

        Assert.Empty(actual);
    }

    [Fact]
    public void
        TryGetFrom__КогдаЛогПустИИндексРавен0ИЛогНачинаетсяСИндекса0__ДолженВернутьTombВДанныхОПредпоследнейЗаписи()
    {
        var (facade, _) = CreateLog(0);
        var expected = LogEntryInfo.Tomb;

        _ = facade.TryGetFrom(0, out _, out var actual);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void TryGetFrom__КогдаЛогПустИИндексРавен0ИЛогНачинаетсяСИндекса0__ДолженВернутьTrue()
    {
        var (facade, _) = CreateLog(0);

        var success = facade.TryGetFrom(0, out _, out _);

        Assert.True(success);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(100)]
    public void TryGetFrom__КогдаЛогПустИИндексРавен0ИЛогНеНачинаетсяСИндекса0__ДолженВернутьFalse(int startIndex)
    {
        var (facade, _) = CreateLog(startIndex);

        var success = facade.TryGetFrom(0, out _, out _);

        Assert.False(success);
    }

    [Theory]
    [InlineData(10, 0)]
    [InlineData(10, 3)]
    [InlineData(10, 9)]
    [InlineData(100, 2)]
    [InlineData(100, 69)]
    public void TryGetFrom__КогдаИндексВнутриЛогаИЕстьТолькоХвостИЛогНачинаетсяС0__ДолженВернутьЗаписиСУказаннойПозиции(
        int logSize,
        int index)
    {
        var term = new Term(1);
        var tailEntries = Enumerable.Range(0, logSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(0, tailEntries);
        var expected = tailEntries.Skip(index).ToArray();

        var success = log.TryGetFrom(index, out var actual, out _);

        Assert.True(success);
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(10, 3)]
    [InlineData(10, 9)]
    [InlineData(100, 2)]
    [InlineData(100, 69)]
    public void
        TryGetFrom__КогдаИндексВнутриЛогаИЕстьТолькоХвостИЛогНачинаетсяС0__ДолженВернутьКорректныеДанныеОПредыдущейЗаписи(
        int logSize,
        int index)
    {
        var term = new Term(1);
        var tailEntries = Enumerable.Range(0, logSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(0, tailEntries);
        var expected = new LogEntryInfo(tailEntries[index - 1].Term, index - 1);

        _ = log.TryGetFrom(index, out _, out var actual);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(10, 3)]
    [InlineData(10, 9)]
    [InlineData(100, 2)]
    [InlineData(100, 69)]
    public void TryGetFrom__КогдаИндексВнутриЛогаИЕстьТолькоХвостИЛогНачинаетсяС0__ДолженВернутьTrue(
        int logSize,
        int index)
    {
        var term = new Term(1);
        var tailEntries = Enumerable.Range(0, logSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(0, tailEntries);
        var expected = new LogEntryInfo(tailEntries[index - 1].Term, index - 1);

        _ = log.TryGetFrom(index, out _, out var actual);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void TryGetFrom__КогдаИндексРавен0ИЕстьТолькоХвостИЛогНачинаетсяС0__ДолженВернутьTrue()
    {
        var term = new Term(1);
        var logSize = 100;
        var tailEntries = Enumerable.Range(0, logSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(0, tailEntries);

        var success = log.TryGetFrom(0, out _, out _);

        Assert.True(success);
    }

    [Fact]
    public void TryGetFrom__КогдаИндексРавен0ИЕстьТолькоХвостИЛогНачинаетсяС0__ДолженВернутьЗаписиИзХвоста()
    {
        var term = new Term(1);
        var logSize = 100;
        var tailEntries = Enumerable.Range(0, logSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(0, tailEntries);

        _ = log.TryGetFrom(0, out var actual, out _);

        Assert.Equal(tailEntries, actual, Comparer);
    }

    [Fact]
    public void TryGetFrom__КогдаИндексРавен0ИЕстьТолькоХвостИЛогНачинаетсяС0__ДолженВернутьTomb()
    {
        var term = new Term(1);
        var logSize = 100;
        var tailEntries = Enumerable.Range(0, logSize)
                                    .Select(i => Entry(term, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(0, tailEntries);
        var expected = LogEntryInfo.Tomb;

        _ = log.TryGetFrom(0, out _, out var actual);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(128)]
    [InlineData(346234562)]
    public void TryGetFrom__КогдаИндексМеньшеНачальногоИзЛога__ДолженВернутьFalse(int startLsn)
    {
        var logSize = 123;
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries);

        var success = log.TryGetFrom(startLsn - 1, out _, out _);

        Assert.False(success);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(136453)]
    public void TryGetFrom__КогдаПереданныйИндексРавенНачальномуИНачальныйИндексНеРавен0__ДолженВернутьFalse(
        int startLsn)
    {
        var logSize = 123;
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries);

        var success = log.TryGetFrom(startLsn, out _, out _);

        Assert.False(success);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void
        TryGetFrom__КогдаПереданныйИндексРавенСледующемуПослеНачальногоИЕстьТолькоХвост__ДолженВернутьВсеЗаписиИзХвоста(
        int startLsn)
    {
        var logSize = 123;
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries);
        var expected = tailEntries.Skip(1);

        _ = log.TryGetFrom(startLsn + 1, out var actual, out _);

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void
        TryGetFrom__КогдаПереданныйИндексРавенСледующемуПослеНачальногоИЕстьТолькоХвост__ДолженВернутьTrue(int startLsn)
    {
        var logSize = 123;
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries);

        var actual = log.TryGetFrom(startLsn + 1, out _, out _);

        Assert.True(actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void
        TryGetFrom__КогдаПереданныйИндексРавенСледующемуПослеНачальногоИЕстьТолькоХвост__ДолженВернутьДанныеОПервойЗаписиВХвосте(
        int startLsn)
    {
        var logSize = 123;
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries);
        var expected = new LogEntryInfo(1, startLsn);

        _ = log.TryGetFrom(startLsn + 1, out _, out var actual);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void
        TryGetFrom__КогдаПереданныйИндексРавенСледующемуПослеНачальногоИЕстьЗакрытыеСегменты__ДолженВернутьДанныеОПервойЗаписиВПервомСегменте(
        int startLsn)
    {
        var logSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);
        var expected = new LogEntryInfo(1, startLsn);

        _ = log.TryGetFrom(startLsn + 1, out _, out var actual);

        Assert.Equal(expected, actual);
    }


    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void
        TryGetFrom__КогдаПереданныйИндексРавенСледующемуПослеНачальногоИЕстьЗакрытыеСегменты__ДолженВернутьВсеЗаписиКромеПервой(
        int startLsn)
    {
        var tailSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(s * segmentSize + e,
                                                                   ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(segmentsCount * segmentSize, tailSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);
        var expected = sealedSegments.SelectMany(s => s).Concat(tailEntries).Skip(1).ToArray();

        _ = log.TryGetFrom(startLsn + 1, out var actual, out _);

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void TryGetFrom__КогдаПереданныйИндексРавенСледующемуПослеНачальногоИЕстьЗакрытыеСегменты__ДолженВернутьTrue(
        int startLsn)
    {
        var logSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);

        var actual = log.TryGetFrom(startLsn + 1, out _, out _);

        Assert.True(actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void TryGetFrom__КогдаПереданныйИндексРавенПервомуВХвостеИЕстьЗакрытыеСегменты__ДолженВернутьЗаписиИзХвоста(
        int startLsn)
    {
        var logSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var index = segmentsCount * segmentSize + startLsn;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);
        var expected = tailEntries;

        _ = log.TryGetFrom(index, out var actual, out _);

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void TryGetFrom__КогдаПереданныйИндексРавенПервомуВХвостеИЕстьЗакрытыеСегменты__ДолженВернутьTrue(
        int startLsn)
    {
        var logSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var index = segmentsCount * segmentSize + startLsn;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);

        var actual = log.TryGetFrom(index, out _, out _);

        Assert.True(actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void
        TryGetFrom__КогдаПереданныйИндексРавенПервомуВХвостеИЕстьЗакрытыеСегменты__ДолженВернутьДанныеОПервойЗаписиИзПоследнегоСегмента(
        int startLsn)
    {
        var logSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(0, segmentSize)
                                                              .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(1, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var index = segmentsCount * segmentSize + startLsn;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);
        var expected = new LogEntryInfo(1, index - 1);

        _ = log.TryGetFrom(index, out _, out var actual);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(6756)]
    public void
        TryGetFrom__КогдаПереданныйИндексРавенУказываетВнутрьЗакрытогоСегмента__ДолженВернутьВсеЗаписиСУказанной(
        int startLsn)
    {
        var logSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(( s * segmentSize + e ),
                                                                   ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(segmentsCount * segmentSize, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var offset = ( segmentsCount - 2 ) * segmentSize + segmentSize / 2;
        var index = offset + startLsn;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);
        var expected = sealedSegments.SelectMany(s => s).Concat(tailEntries).Skip(offset);

        _ = log.TryGetFrom(index, out var actual, out _);

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(666666)]
    public void
        TryGetFrom__КогдаПереданныйИндексРавенНачальномуВЗакрытомСегменте__ДолженВернутьВсеЗаписиПослеЭтогоИндекса(
        int startLsn)
    {
        var logSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(( s * segmentSize + e ),
                                                                   ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(segmentsCount * segmentSize, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var offset = segmentsCount / 2 * segmentSize;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);
        var expected = sealedSegments.SelectMany(s => s).Concat(tailEntries).Skip(offset);

        _ = log.TryGetFrom(offset + startLsn, out var actual, out _);

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(666666)]
    public void TryGetFrom__КогдаПереданныйИндексРавенНачальномуВЗакрытомСегменте__ДолженВернутьTrue(
        int startLsn)
    {
        var logSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(( s * segmentSize + e ),
                                                                   ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(segmentsCount * segmentSize, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var offset = segmentsCount / 2 * segmentSize;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);

        var actual = log.TryGetFrom(offset + startLsn, out _, out _);

        Assert.True(actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(666666)]
    public void
        TryGetFrom__КогдаПереданныйИндексРавенНачальномуВЗакрытомСегменте__ДолженВернутьДанныеОПоследнейЗаписиВПредыдущемСегменте(
        int startLsn)
    {
        var logSize = 123;
        var segmentsCount = 12;
        var segmentSize = 64;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(( s * segmentSize + e ),
                                                                   ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(segmentsCount * segmentSize, logSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var segmentIndex = segmentsCount / 2;
        var offset = segmentIndex * segmentSize;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);
        var expected = new LogEntryInfo(segmentIndex * segmentSize, segmentIndex * segmentSize - 1 + startLsn);

        _ = log.TryGetFrom(offset + startLsn, out _, out var actual);

        Assert.Equal(expected, actual);
    }
}