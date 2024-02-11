using System.IO.Abstractions;
using System.Text;
using Serilog.Core;
using TaskFlux.Consensus;
using TaskFlux.Persistence.Log;
using Xunit;

namespace TaskFlux.Persistence.Tests;

[Trait("Category", "Persistence")]
public class SegmentedFileLogTests : IDisposable
{
    private static LogEntry Entry(int term, string data)
        => new(new Term(term), Encoding.UTF8.GetBytes(data));

    private static LogEntry Entry(Term term, string data)
        => new(term, Encoding.UTF8.GetBytes(data));

    private static SegmentedFileLogOptions TestOptions => new(softLimit: long.MaxValue,
        hardLimit: long.MaxValue,
        preallocateSegment: false);

    private record MockFiles(IDirectoryInfo LogDirectory, IDirectoryInfo DataDirectory);

    /// <summary>
    /// Файлы, который создаются вызовом <see cref="CreateLog()"/>.
    /// Это поле используется для проверки того, что после операции файл остается в корректном состоянии.
    /// </summary>
    private MockFiles? _createdFiles;

    private SegmentedFileLog? _createdFileLog;

    public void Dispose()
    {
        // Дополнительная проверка, что оставляемые файлы находятся в корректном состоянии
        if (_createdFiles is var (_, dataDir))
        {
            var ex = Record.Exception(() => SegmentedFileLog.Initialize(dataDir, Logger.None, TestOptions));
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
        return CreateLog(0);
        // var fileSystem = Helpers.CreateFileSystem();
        // var mockFiles = new MockFiles(fileSystem.Log, fileSystem.DataDirectory);
        // _createdFiles = mockFiles;
        // var fileLog = SegmentedFileLog.InitializeTest(fileSystem.DataDirectory, );
        // _createdFileLog = fileLog;
        // return ( fileLog, mockFiles );
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
            return options ?? TestOptions;
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

        log.InsertRangeOverwrite(appended, log.LastRecordIndex + 1);

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
    [InlineData(0, 0, 0, 1)]
    [InlineData(0, 1, 1, 1)]
    [InlineData(0, 0, 0, 3)]
    [InlineData(0, 0, 10, 3)]
    [InlineData(0, 1, 0, 3)]
    [InlineData(0, 1, 1, 2)]
    [InlineData(0, 1, 1, 5)]
    [InlineData(0, 1, 18, 5)]
    [InlineData(0, 3, 30, 5)]
    [InlineData(10, 0, 1, 5)]
    [InlineData(10, 0, 1, 3)]
    public void InsertRangeOverwrite__КогдаПревышаетсяЖесткийПределИВставкаВКонец__ДолженУспешноВставитьЗаписи(
        int startLsn,
        int segmentsCount,
        int tailSize,
        int segmentsToInsertCount)
    {
        const int softLimit = 512;
        const int hardLimit = 1024;
        const int segmentSize = 128;

        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(e => Entry(1, e.ToString()))
                                    .ToArray();

        var segments = Enumerable.Range(0, segmentsCount)
                                 .Select(s => Enumerable.Range(1, segmentSize)
                                                        .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                        .ToArray())
                                 .ToArray();
        var toInsert = Enumerable.Range(0, segmentsToInsertCount)
                                 .SelectMany(_ => SegmentedFileLog.GenerateEntriesForSizeAtLeast(hardLimit, 1))
                                 .ToArray();
        var (log, _) = CreateLog(start: startLsn,
            tailEntries: tailEntries,
            segmentEntries: segments,
            options: new SegmentedFileLogOptions(softLimit, hardLimit, preallocateSegment: true));
        var lastIndex = startLsn + segmentSize * segmentsCount + tailSize;
        var expected = segments.SelectMany(s => s).Concat(tailEntries).Concat(toInsert);

        log.InsertRangeOverwrite(toInsert, lastIndex);

        Assert.Equal(expected, log.ReadAllTest(), Comparer);
    }

    [Theory]
    [InlineData(0, 1, 1, 1)]
    [InlineData(0, 0, 10, 3)]
    [InlineData(0, 1, 15, 2)]
    [InlineData(0, 1, 11, 5)]
    [InlineData(0, 1, 18, 5)]
    [InlineData(0, 3, 30, 5)]
    [InlineData(10, 0, 19, 5)]
    [InlineData(10, 0, 1, 3)]
    [InlineData(10, 3, 30, 5)]
    public void InsertRangeOverwrite__КогдаПревышаетсяЖесткийПределИВставкаВСерединуХвоста__ДолженУспешноВставитьЗаписи(
        int startLsn,
        int segmentsCount,
        int tailSize,
        int segmentsToInsertCount)
    {
        const int softLimit = 512;
        const int hardLimit = 1024;
        const int segmentSize = 128;

        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(e => Entry(1, e.ToString()))
                                    .ToArray();

        var segments = Enumerable.Range(0, segmentsCount)
                                 .Select(s => Enumerable.Range(1, segmentSize)
                                                        .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                        .ToArray())
                                 .ToArray();
        var toInsert = Enumerable.Range(0, segmentsToInsertCount)
                                 .SelectMany(_ => SegmentedFileLog.GenerateEntriesForSizeAtLeast(hardLimit, 1))
                                 .ToArray();
        var (log, _) = CreateLog(start: startLsn,
            tailEntries: tailEntries,
            segmentEntries: segments,
            options: new SegmentedFileLogOptions(softLimit, hardLimit, preallocateSegment: true));
        var insertIndex = startLsn + segmentSize * segmentsCount + tailSize / 2;
        var expected = segments.SelectMany(s => s)
                               .Concat(tailEntries.Take(tailSize / 2))
                               .Concat(toInsert);

        log.InsertRangeOverwrite(toInsert, insertIndex);

        Assert.Equal(expected, log.ReadAllTest(), Comparer);
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

        log.InsertRangeOverwrite(toInsert, log.LastRecordIndex + 1);

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
        var options = new SegmentedFileLogOptions(softLimit, hardLimit, preallocateSegment: true);
        // На всякий случай, сделаем побольше чуть-чуть
        var atLeastSizeBytes = softLimit + 10;
        var initialTailEntries = SegmentedFileLog.GenerateEntriesForSizeAtLeast(atLeastSizeBytes, 1);
        var (log, _) = CreateLog(0, initialTailEntries, options: options);
        var toInsert = Enumerable.Range(( int ) initialTailEntries[^1].Term.Value + 1, 10)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        log.SetCommitIndexTest(log.LastRecordIndex);

        log.InsertRangeOverwrite(toInsert, log.LastRecordIndex + 1);

        Assert.Equal(2, log.GetSegmentsCountTest());
        Assert.Equal(toInsert, log.ReadTailTest());
    }

    [Fact]
    public void InsertRangeOverwrite__КогдаПревышенЖесткийПредел__ДолженНачатьНовыйСегмент()
    {
        const long softLimit = 1024;     // 1 Кб
        const long hardLimit = 1024 * 2; // 2 Кб
        var options = new SegmentedFileLogOptions(softLimit, hardLimit, preallocateSegment: true);
        var initialTailEntries = SegmentedFileLog.GenerateEntriesForSizeAtLeast(hardLimit, 1);
        var (log, _) = CreateLog(0, initialTailEntries, options: options);
        var toInsert = Enumerable.Range(( int ) initialTailEntries[^1].Term.Value + 1, 10)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        // Как будто ничего не закоммичено
        // log.SetCommitIndexTest(log.LastIndex);

        log.InsertRangeOverwrite(toInsert, log.LastRecordIndex + 1);

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
        var insertIndex = log.LastRecordIndex + indexGap + 1;

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
        var insertIndex = log.LastRecordIndex + indexGap + 1;

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
        var insertIndex = log.LastRecordIndex + indexGap + 1;
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
    public void TryGetLastLogEntry__КогдаЛогПустИНачинаетсяС0__ДолженВернутьTomb()
    {
        var expected = LogEntryInfo.Tomb;
        var (log, _) = CreateLog();

        var success = log.TryGetLastLogEntry(out var actual);

        Assert.True(success);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(87654)]
    public void TryGetLastLogEntry__КогдаЛогПустИНачинаетсяНеС0__ДолженВернутьFalse(int startLsn)
    {
        var (log, _) = CreateLog(start: startLsn);

        var actual = log.TryGetLastLogEntry(out _);

        Assert.False(actual);
    }

    [Theory]
    [InlineData(0, 0, 1)]
    [InlineData(0, 0, 100)]
    [InlineData(0, 1, 1)]
    [InlineData(0, 1, 100)]
    [InlineData(1, 0, 1)]
    [InlineData(1, 0, 100)]
    [InlineData(1, 1, 100)]
    public void TryGetLastLogEntry__КогдаЕстьЗакрытыеСегментыИХвостНеПуст__ДолженВернутьЗаписьИзХвоста(
        int startLsn,
        int segmentsCount,
        int tailSize)
    {
        const int segmentSize = 128;
        var segmentEntries = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(0, tailSize)
                                    .Select(e => Entry(1, ( segmentsCount * segmentSize + e ).ToString()))
                                    .ToArray();
        var (log, _) = CreateLog(start: startLsn,
            tailEntries: tailEntries,
            segmentEntries: segmentEntries);
        var expected = new LogEntryInfo(1, startLsn + segmentSize * segmentsCount + tailSize - 1);

        var success = log.TryGetLastLogEntry(out var actual);

        Assert.True(success);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(0, 2)]
    [InlineData(0, 36)]
    [InlineData(1, 544)]
    [InlineData(1, 4)]
    [InlineData(176, 1)]
    [InlineData(176, 55)]
    public void TryGetLastLogEntry__КогдаЕстьТолькоЗакрытыйСегментИХвостПуст__ДолженВернутьПоследнююЗаписьИзСегмента(
        int startLsn,
        int segmentSize)
    {
        var initial = Enumerable.Range(1, segmentSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();

        var (log, _) = CreateLog(start: startLsn,
            tailEntries: Array.Empty<LogEntry>(),
            segmentEntries: new[] {initial});
        var expected = new LogEntryInfo(initial[^1].Term, startLsn + initial.Length - 1);

        var success = log.TryGetLastLogEntry(out var actual);
        Assert.True(success);
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

        var actual = log.ReadDataRange(0, log.LastRecordIndex).ToList();

        Assert.Equal(expected.Select(e => e.Data), actual, ByteComparer);
    }

    [Theory]
    [InlineData(0, 1, 0)]
    [InlineData(0, 1, 1)]
    [InlineData(0, 1, 100)]
    [InlineData(0, 2, 0)]
    [InlineData(0, 2, 1)]
    [InlineData(0, 2, 100)]
    [InlineData(1, 10, 0)]
    [InlineData(1, 10, 1)]
    [InlineData(1, 10, 100)]
    public void ReadDataRange__КогдаЕстьЗакрытыеСегментыИИндексРавенНачальному__ДолженСчитатьВесьЛог(
        int startLsn,
        int segmentsCount,
        int tailSize)
    {
        var segmentSize = 128;
        var tail = Enumerable.Range(0, tailSize)
                             .Select(i => Entry(1, i.ToString()))
                             .ToList();
        var segments = Enumerable.Range(0, segmentsCount)
                                 .Select(s => Enumerable.Range(1, segmentSize)
                                                        .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                        .ToArray())
                                 .ToArray();

        var (log, _) = CreateLog(startLsn, tailEntries: tail, segmentEntries: segments);
        var expected = segments.SelectMany(s => s).Concat(tail);

        var actual = log.ReadDataRange(log.StartIndex, log.LastRecordIndex).ToList();

        Assert.Equal(expected.Select(e => e.Data), actual, ByteComparer);
    }

    [Theory]
    [InlineData(0, 1, 1)]
    [InlineData(0, 1, 100)]
    [InlineData(0, 2, 1)]
    [InlineData(0, 2, 100)]
    [InlineData(1, 10, 1)]
    [InlineData(1, 10, 100)]
    public void ReadDataRange__КогдаЕстьЗакрытыеСегментыИИндексРавенПервомуВХвосте__ДолженСчитатьЗаписиВХвосте(
        int startLsn,
        int segmentsCount,
        int tailSize)
    {
        var segmentSize = 128;
        var tail = Enumerable.Range(0, tailSize)
                             .Select(i => Entry(1, i.ToString()))
                             .ToList();
        var segments = Enumerable.Range(0, segmentsCount)
                                 .Select(s => Enumerable.Range(1, segmentSize)
                                                        .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                        .ToArray())
                                 .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries: tail, segmentEntries: segments);
        var expected = tail;
        var startIndex = startLsn + segmentSize * segmentsCount;

        var actual = log.ReadDataRange(startIndex, log.LastRecordIndex).ToList();

        Assert.Equal(expected.Select(e => e.Data), actual, ByteComparer);
    }

    [Theory]
    [InlineData(0, 1, 1)]
    [InlineData(0, 1, 100)]
    [InlineData(0, 2, 1)]
    [InlineData(0, 2, 100)]
    [InlineData(1, 10, 1)]
    [InlineData(1, 10, 100)]
    public void
        ReadDataRange__КогдаЕстьЗакрытыеСегментыИИндексВнутриСегмента__ДолженСчитатьЗаписиНачинаяСУказанногоИндекса(
        int startLsn,
        int segmentsCount,
        int tailSize)
    {
        var segmentSize = 128;
        var tail = Enumerable.Range(0, tailSize)
                             .Select(i => Entry(1, i.ToString()))
                             .ToList();
        var segments = Enumerable.Range(0, segmentsCount)
                                 .Select(s => Enumerable.Range(1, segmentSize)
                                                        .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                        .ToArray())
                                 .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries: tail, segmentEntries: segments);
        var offset = segmentSize * ( segmentsCount / 2 ) + segmentSize / 2;
        var startIndex = startLsn + offset;
        var expected = segments.SelectMany(s => s).Concat(tail).Skip(offset).ToArray();

        var actual = log.ReadDataRange(startIndex, log.LastRecordIndex).ToList();

        Assert.Equal(expected.Select(e => e.Data), actual, ByteComparer);
    }

    [Theory]
    [InlineData(0, 1, 1)]
    [InlineData(0, 1, 100)]
    [InlineData(0, 2, 1)]
    [InlineData(0, 2, 100)]
    [InlineData(1, 10, 1)]
    [InlineData(1, 10, 100)]
    public void ReadDataRange__КогдаЕстьЗакрытыеСегментыИИндексВНачалеСегмент__ДолженСчитатьВсеЗаписиСНужногоСегмента(
        int startLsn,
        int segmentsCount,
        int tailSize)
    {
        var segmentSize = 128;
        var tail = Enumerable.Range(0, tailSize)
                             .Select(i => Entry(1, i.ToString()))
                             .ToList();
        var segments = Enumerable.Range(0, segmentsCount)
                                 .Select(s => Enumerable.Range(1, segmentSize)
                                                        .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                        .ToArray())
                                 .ToArray();
        var (log, _) = CreateLog(startLsn, tailEntries: tail, segmentEntries: segments);
        var offset = segmentSize * ( segmentsCount / 2 );
        var startIndex = startLsn + offset;
        var expected = segments.SelectMany(s => s).Concat(tail).Skip(offset).ToArray();

        var actual = log.ReadDataRange(startIndex, log.LastRecordIndex).ToList();

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
        DeleteCoveredSegmentsUntil__КогдаВЛогеЕстьНесколькоЗакрытыхСегментовИИндексРавенНачальномуВХвосте__ДолженУдалитьВсеЗакрытыеСегменты(
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

        facade.DeleteCoveredSegmentsUntil(startSegmentIndex);

        Assert.Equal(1, facade.GetSegmentsCountTest());
    }

    [Fact]
    public void
        DeleteCoveredSegmentsUntil__КогдаИндексУказываетНаСегментВСередине__ДолженУдалитьСегментыДоЭтогоСегмента()
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

        facade.DeleteCoveredSegmentsUntil(startSegmentIndex);

        Assert.Equal(expectedSegmentsCount, facade.GetSegmentsCountTest());
    }

    [Fact]
    public void DeleteCoveredSegmentsUntil__КогдаИндексРавенНачальному__НеДолженУдалятьСегменты()
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

        facade.DeleteCoveredSegmentsUntil(startSegmentIndex);

        Assert.Equal(expectedSegmentsCount, facade.GetSegmentsCountTest());
    }

    [Fact]
    public void DeleteCoveredSegmentsUntil__КогдаБылиУдаленыСегменты__ДолженОбновитьНачальныйИндекс()
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

        facade.DeleteCoveredSegmentsUntil(startSegmentIndex);

        Assert.Equal(expectedStartIndex, facade.StartIndex);
    }

    [Fact]
    public void DeleteCoveredSegmentsUntil__КогдаИндексБольшеПоследнего__ДолженУдалитьВсеСегменты()
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

        facade.DeleteCoveredSegmentsUntil(deleteIndex);

        Assert.Equal(1, facade.GetSegmentsCountTest());
        Assert.Empty(facade.ReadAllTest());
    }

    [Fact]
    public void DeleteCoveredSegmentsUntil__КогдаИндексБольшеПоследнего__ДолженОбновитьНачальныйИндекс()
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

        facade.DeleteCoveredSegmentsUntil(deleteIndex);

        Assert.Equal(deleteIndex, facade.StartIndex);
    }

    [Fact]
    public void
        DeleteCoveredSegmentsUntil__КогдаИндексУказываетНаЗаписьВнутриПервогоЗакрытогоСегмента__НеДолженУдалятьСегменты()
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

        facade.DeleteCoveredSegmentsUntil(deleteIndex);

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

    [Theory]
    [InlineData(0, 1, 0)]
    [InlineData(0, 1, 1)]
    [InlineData(0, 2, 1)]
    [InlineData(0, 2, 10)]
    [InlineData(0, 123, 10)]
    [InlineData(1, 1, 0)]
    [InlineData(1, 1, 1)]
    [InlineData(1, 2, 1)]
    [InlineData(1, 123, 1)]
    [InlineData(666, 1, 0)]
    [InlineData(666, 1, 1)]
    [InlineData(666, 2, 1)]
    [InlineData(666, 123, 1)]
    public void TryGetFrom__КогдаИндексСледующийПослеПоследнегоИХвостНеПуст__ДолженВернутьПустойМассив(
        int startLsn,
        int tailSize,
        int segmentsCount)
    {
        var segmentSize = 128;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(s * segmentSize + e,
                                                                   ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(segmentsCount * segmentSize + 1, tailSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var offset = segmentsCount * segmentSize + tailSize;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);

        _ = log.TryGetFrom(startLsn + offset, out var actual, out _);

        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(0, 1, 0)]
    [InlineData(0, 1, 1)]
    [InlineData(0, 2, 1)]
    [InlineData(0, 2, 10)]
    [InlineData(0, 123, 10)]
    [InlineData(1, 1, 0)]
    [InlineData(1, 1, 1)]
    [InlineData(1, 2, 1)]
    [InlineData(1, 123, 1)]
    [InlineData(666, 1, 0)]
    [InlineData(666, 1, 1)]
    [InlineData(666, 2, 1)]
    [InlineData(666, 123, 1)]
    public void TryGetFrom__КогдаИндексСледующийПослеПоследнегоИХвостНеПуст__ДолженВернутьДанныеОПоследнейЗаписиВХвосте(
        int startLsn,
        int tailSize,
        int segmentsCount)
    {
        var segmentSize = 128;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(s * segmentSize + e,
                                                                   ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(segmentsCount * segmentSize + 1, tailSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var offset = segmentsCount * segmentSize + tailSize;
        var lastIndex = startLsn + offset - 1;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);
        var expected = new LogEntryInfo(tailEntries[^1].Term, lastIndex);

        _ = log.TryGetFrom(lastIndex + 1, out _, out var actual);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0, 1, 0)]
    [InlineData(0, 1, 1)]
    [InlineData(0, 2, 1)]
    [InlineData(0, 2, 10)]
    [InlineData(0, 123, 10)]
    [InlineData(1, 1, 0)]
    [InlineData(1, 1, 1)]
    [InlineData(1, 2, 1)]
    [InlineData(1, 123, 1)]
    [InlineData(666, 1, 0)]
    [InlineData(666, 1, 1)]
    [InlineData(666, 2, 1)]
    [InlineData(666, 123, 1)]
    public void TryGetFrom__КогдаИндексСледующийПослеПоследнегоИХвостНеПуст__ДолженВернутьTrue(
        int startLsn,
        int tailSize,
        int segmentsCount)
    {
        var segmentSize = 128;
        var sealedSegments = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(s * segmentSize + e,
                                                                   ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(segmentsCount * segmentSize + 1, tailSize)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var offset = segmentsCount * segmentSize + tailSize;
        var lastIndex = startLsn + offset - 1;
        var (log, _) = CreateLog(startLsn, tailEntries, sealedSegments);

        var actual = log.TryGetFrom(lastIndex + 1, out _, out _);

        Assert.True(actual);
    }

    [Fact]
    public void Инициализация__КогдаДиректорияПуста__ДолженСоздатьПустойХвост()
    {
        var fs = Helpers.CreateFileSystem();

        var log = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);

        Assert.Empty(log.ReadTailTest());
    }

    [Fact]
    public void Инициализация__КогдаДиректорияПуста__ДолженИнициализироватьСегментС0()
    {
        var fs = Helpers.CreateFileSystem();

        var log = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);

        Assert.Equal(0, log.StartIndex);
    }

    [Fact]
    public void Инициализация__КогдаДиректорияПуста__ДолженСоздатьФайлСегмента()
    {
        var fs = Helpers.CreateFileSystem();
        var expectedFileName = new SegmentFileName(0);

        _ = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);

        Assert.Contains(fs.Log.GetFiles(), file => file.Name == expectedFileName.GetFileName());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(87654)]
    [InlineData(100000)]
    public void
        Инициализация__КогдаСуществовалПустойФайлСегмента__ДолженКорректноИнициализироватьПервыйНачальныйИндексСегмента(
        int startLsn)
    {
        var fs = Helpers.CreateFileSystem();
        var oldLog = SegmentedFileLog.InitializeTest(fs.Log,
            startIndex: startLsn,
            tailEntries: Array.Empty<LogEntry>(),
            segmentEntries: Array.Empty<IReadOnlyList<LogEntry>>());
        oldLog.Dispose();

        var newLog = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);

        Assert.Equal(startLsn, newLog.StartIndex);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100000)]
    [InlineData(87654)]
    public void Инициализация__КогдаСуществовалПустойФайлСегмента__ДолженИнициализироватьПустойЛог(int startLsn)
    {
        var fs = Helpers.CreateFileSystem();

        var oldLog = SegmentedFileLog.InitializeTest(fs.DataDirectory,
            startIndex: startLsn,
            tailEntries: Array.Empty<LogEntry>(),
            segmentEntries: Array.Empty<IReadOnlyList<LogEntry>>());
        oldLog.Dispose();
        var newLog = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);

        Assert.Empty(newLog.ReadTailTest());
    }

    [Theory]
    [InlineData(0, 1, 0)]
    [InlineData(0, 1, 1)]
    [InlineData(1, 1, 1)]
    [InlineData(0, 2, 0)]
    [InlineData(3765, 10, 10)]
    public void Инициализация__КогдаБылиЗакрытыеСегменты__ДолженИнициализироватьВсеЗаписиКорректно(
        int startIndex,
        int segmentsCount,
        int tailSize)
    {
        const int segmentSize = 128;
        var segmentEntries = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(segmentsCount * segmentSize, tailSize)
                                    .Select(e => Entry(1, e.ToString()))
                                    .ToArray();
        var fs = Helpers.CreateFileSystem();
        var oldLog = SegmentedFileLog.InitializeTest(fs.Log, startIndex, tailEntries: tailEntries,
            segmentEntries: segmentEntries);
        oldLog.Dispose();
        var expected = segmentEntries.SelectMany(s => s).Concat(tailEntries);

        var newLog = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);

        var actual = newLog.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(0, 100)]
    [InlineData(1, 1)]
    [InlineData(1, 100)]
    [InlineData(674565, 62)]
    public void Инициализация__КогдаЕстьТолькоХвостИОнНеПуст__ДолженСчитатьСохраненныеЗаписи(
        int startIndex,
        int tailSize)
    {
        using var memory = new MemoryStream();
        var tailEntries = Enumerable.Range(1, tailSize)
                                    .Select(i => Entry(i, $"data {i}"))
                                    .ToArray();

        var fs = Helpers.CreateFileSystem();
        var oldLog = SegmentedFileLog.InitializeTest(logDirectory: fs.Log,
            startIndex: startIndex,
            tailEntries: tailEntries,
            segmentEntries: Array.Empty<IReadOnlyList<LogEntry>>());
        oldLog.Dispose();
        var expected = tailEntries;

        var newLog = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);
        var actual = newLog.ReadAllTest();

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(0, 100)]
    [InlineData(1, 1)]
    [InlineData(1, 100)]
    [InlineData(674565, 62)]
    public void Инициализация__КогдаЕстьТолькоХвостИОнНеПуст__ДолженКорректноИнициализироватьНачальныйИндекс(
        int startIndex,
        int tailSize)
    {
        var tailEntries = Enumerable.Range(1, tailSize)
                                    .Select(i => Entry(i, $"data {i}"))
                                    .ToArray();

        var fs = Helpers.CreateFileSystem();
        var oldLog = SegmentedFileLog.InitializeTest(logDirectory: fs.Log,
            startIndex: startIndex,
            tailEntries: tailEntries,
            segmentEntries: Array.Empty<IReadOnlyList<LogEntry>>());
        oldLog.Dispose();
        var expected = new Lsn(startIndex);

        var newLog = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);

        var actual = newLog.StartIndex;
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0, 1, 1)]
    [InlineData(0, 2, 1)]
    [InlineData(0, 10, 1)]
    [InlineData(0, 1, 100)]
    [InlineData(0, 2, 100)]
    [InlineData(1, 1, 1)]
    [InlineData(1, 1, 100)]
    [InlineData(1, 6, 1)]
    [InlineData(1, 6, 75)]
    [InlineData(786543, 1, 1)]
    [InlineData(786543, 1, 16)]
    [InlineData(786543, 6, 16)]
    public void Инициализация__КогдаЕстьЗакрытыеСегментыИНеПустойХвост__ДолженКорректноИнициализироватьВсеЗаписи(
        int startIndex,
        int segmentsCount,
        int tailSize)
    {
        var random = new Random(HashCode.Combine(startIndex, segmentsCount, tailSize));
        const int maxSegmentSize = 256;
        var tailEntries = Enumerable.Range(1, tailSize)
                                    .Select(i => Entry(i, $"data {i}"))
                                    .ToArray();
        var i = 0;
        var segmentEntries = Enumerable.Range(0, segmentsCount)
                                       .Select(_ => Enumerable.Range(1, random.Next(1, maxSegmentSize))
                                                              .Select(_ => Entry(1, ( i++ ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var fs = Helpers.CreateFileSystem();
        var oldLog = SegmentedFileLog.InitializeTest(logDirectory: fs.Log,
            startIndex: startIndex,
            tailEntries: tailEntries,
            segmentEntries: segmentEntries);
        oldLog.Dispose();
        var expected = segmentEntries.SelectMany(s => s).Concat(tailEntries);

        var newLog = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);

        var actual = newLog.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(0, 0, 0)]
    [InlineData(0, 1, 0)]
    [InlineData(0, 4, 0)]
    [InlineData(0, 0, 1)]
    [InlineData(0, 1, 1)]
    [InlineData(0, 2, 1)]
    [InlineData(0, 10, 1)]
    [InlineData(0, 1, 100)]
    [InlineData(0, 2, 100)]
    [InlineData(1, 1, 1)]
    [InlineData(1, 1, 100)]
    [InlineData(1, 6, 1)]
    [InlineData(1, 6, 75)]
    [InlineData(7863, 1, 1)]
    [InlineData(64643, 1, 16)]
    [InlineData(99999, 6, 16)]
    public void
        Инициализация__КогдаЕстьЗакрытыеСегментыИНеПустойХвост__ДолженВыставитьИндексКоммитаВПредшествующийНачальномуИндекс(
        int startIndex,
        int segmentsCount,
        int tailSize)
    {
        var random = new Random(HashCode.Combine(startIndex, segmentsCount, tailSize));
        const int maxSegmentSize = 256;
        var tailEntries = Enumerable.Range(1, tailSize)
                                    .Select(i => Entry(i, $"data {i}"))
                                    .ToArray();
        var i = 0;
        var segmentEntries = Enumerable.Range(0, segmentsCount)
                                       .Select(_ => Enumerable.Range(1, random.Next(1, maxSegmentSize))
                                                              .Select(_ => Entry(1, ( i++ ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var fs = Helpers.CreateFileSystem();
        var oldLog = SegmentedFileLog.InitializeTest(logDirectory: fs.Log,
            startIndex: startIndex,
            tailEntries: tailEntries,
            segmentEntries: segmentEntries);
        oldLog.Dispose();
        var expected = new Lsn(startIndex - 1);

        var newLog = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, TestOptions);

        var actual = newLog.CommitIndex;
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(128)]
    [InlineData(512)]
    [InlineData(1024)]
    public void Инициализация__КогдаФайловСегментовНеБыло__ДолженСоздатьНовыйФайлСегментаСРазмеромБольшеМягкогоПредела(
        int softLimit)
    {
        var fs = Helpers.CreateFileSystem();
        var firstLogFile = new SegmentFileName(0);
        var options = new SegmentedFileLogOptions(softLimit, softLimit * 2, preallocateSegment: true);
        _ = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, options);

        var fileInfo = fs.Log.GetFiles().Single(f => f.Name == firstLogFile.GetFileName());
        Assert.True(softLimit <= fileInfo.Length, "softLimit < fileInfo.Length");
    }

    [Theory]
    [InlineData(128)]
    [InlineData(512)]
    [InlineData(1024)]
    public void
        Инициализация__КогдаФайловСегментовНеБыло__ДолженСоздатьНовыйФайлСегментаСРазмеромНеБольшеЖесткогоПредела(
        int hardLimit)
    {
        var fs = Helpers.CreateFileSystem();
        var firstLogFile = new SegmentFileName(0);
        var options = new SegmentedFileLogOptions(hardLimit / 2, hardLimit, true);
        _ = SegmentedFileLog.Initialize(fs.DataDirectory, Logger.None, options);

        var fileInfo = fs.Log.GetFiles().Single(f => f.Name == firstLogFile.GetFileName());
        Assert.True(fileInfo.Length < hardLimit, "fileInfo.Length < hardLimit");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(8434)]
    public void CommitIndex__КогдаЛогИнициализировался__ДолженВыставитьИндексКоммитаВПредыдущийОтНачального(
        int startLsn)
    {
        var (log, _) = CreateLog(startLsn, tailEntries: new[] {Entry(1, "sample")});
        var expected = new Lsn(startLsn - 1);

        var actual = log.CommitIndex;

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(100)]
    public void StartNewWith__КогдаЛогБылПуст__ДолженСоздатьНовыйФайл(int startLsn)
    {
        var (log, fs) = CreateLog();
        var expectedFileName = new SegmentFileName(startLsn);

        log.StartNewWith(startLsn);

        Assert.Contains(fs.LogDirectory.GetFiles(), file => file.Name == expectedFileName.GetFileName());
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(0, 2)]
    [InlineData(0, 100)]
    [InlineData(0, 1024)]
    [InlineData(123, 124)]
    [InlineData(512, 1024)]
    public void StartNewWith__КогдаЛогБылПуст__ДолженУдалитьСтарыйФайл(int oldStartLsn, int newStartLsn)
    {
        var (log, fs) = CreateLog(start: oldStartLsn);
        var oldSegmentFile = new SegmentFileName(oldStartLsn);

        log.StartNewWith(newStartLsn);

        Assert.DoesNotContain(fs.LogDirectory.GetFiles(), file => file.Name == oldSegmentFile.GetFileName());
    }

    [Theory]
    [InlineData(0, 0, 1, 2)]
    [InlineData(0, 1, 0, 23)]
    [InlineData(0, 1, 1, 44)]
    [InlineData(1, 0, 1, 654)]
    [InlineData(1, 1, 0, 10)]
    [InlineData(76, 7, 12, 10)]
    public void StartNewWith__КогдаВЛогеБылиЗаписи__ДолженУдалитьСтарыеФайлы(
        int oldStartLsn,
        int segmentsCount,
        int tailSize,
        int newStartLsn)
    {
        var tail = Enumerable.Range(0, tailSize)
                             .Select(e => Entry(1, e.ToString()))
                             .ToArray();
        const int segmentSize = 128;
        var segments = Enumerable.Range(0, segmentsCount)
                                 .Select(s => Enumerable.Range(1, segmentSize)
                                                        .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                        .ToArray())
                                 .ToArray();
        var (log, fs) = CreateLog(start: oldStartLsn,
            tailEntries: tail,
            segmentEntries: segments);
        var oldSegmentFileNames = log.GetAllSegmentNamesTest()
                                     .Select(n => n.GetFileName())
                                     .ToHashSet(StringComparer.InvariantCultureIgnoreCase);

        log.StartNewWith(newStartLsn);

        Assert.DoesNotContain(fs.LogDirectory.GetFiles(), file => oldSegmentFileNames.Contains(file.Name));
    }

    [Theory]
    [InlineData(0, 0, 1, 2)]
    [InlineData(0, 1, 0, 23)]
    [InlineData(0, 1, 1, 44)]
    [InlineData(1, 0, 1, 654)]
    [InlineData(1, 1, 0, 10)]
    [InlineData(76, 7, 12, 10)]
    public void StartNewWith__КогдаВЛогеБылиЗаписи__ДолженВыставитьИндексКоммитаВПредыдущийОтНачального(
        int oldStartLsn,
        int segmentsCount,
        int tailSize,
        int newStartLsn)
    {
        var tail = Enumerable.Range(0, tailSize)
                             .Select(e => Entry(1, e.ToString()))
                             .ToArray();
        const int segmentSize = 128;
        var segments = Enumerable.Range(0, segmentsCount)
                                 .Select(s => Enumerable.Range(1, segmentSize)
                                                        .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                        .ToArray())
                                 .ToArray();
        var (log, _) = CreateLog(start: oldStartLsn,
            tailEntries: tail,
            segmentEntries: segments);
        var expected = new Lsn(newStartLsn - 1);

        log.StartNewWith(newStartLsn);
        var actual = log.CommitIndex;

        Assert.Equal(expected, actual);
    }
}