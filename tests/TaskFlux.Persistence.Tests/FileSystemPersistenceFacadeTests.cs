using System.IO.Abstractions;
using System.Text;
using Serilog.Core;
using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Persistence.Log;
using TaskFlux.Persistence.Metadata;
using TaskFlux.Persistence.Snapshot;
using Xunit;

// ReSharper disable UseUtf8StringLiteral
// ReSharper disable StringLiteralTypo

namespace TaskFlux.Persistence.Tests;

[Trait("Category", "Persistence")]
public class FileSystemPersistenceFacadeTests : IDisposable
{
    private static LogEntry EmptyEntry(int term) => new(new Term(term), Array.Empty<byte>());

    private record MockDataFileSystem(
        IDirectoryInfo LogFile,
        IFileInfo MetadataFile,
        IFileInfo SnapshotFile,
        IDirectoryInfo TemporaryDirectory,
        IDirectoryInfo DataDirectory);

    private MockDataFileSystem? _createdFs;

    public void Dispose()
    {
        if (_createdFs is not var (_, _, _, _, dataDir))
        {
            return;
        }

        Assert.Null(Record.Exception(() => SegmentedFileLog.Initialize(dataDir, Logger.None)));
        Assert.Null(Record.Exception(() => MetadataFile.Initialize(dataDir)));
        Assert.Null(Record.Exception(() => SnapshotFile.Initialize(dataDir)));
    }

    private static readonly Term DefaultTerm = Term.Start;

    /// <summary>
    /// Метод для создания фасада с файлами в памяти.
    /// Создает и инициализирует нужную структуру файлов в памяти.
    /// </summary>
    /// <remarks>Создаваемые файлы пустые</remarks>
    private (FileSystemPersistenceFacade Facade, MockDataFileSystem Fs) CreateFacade(
        Term? initialTerm = null,
        NodeId? votedFor = null,
        Lsn? startLsn = null,
        IReadOnlyList<LogEntry>? tailEntries = null,
        IReadOnlyList<IReadOnlyList<LogEntry>>? segmentEntries = null,
        SnapshotOptions? snapshotOptions = null,
        (Term, Lsn, ISnapshot)? snapshotData = null,
        Lsn? commitIndex = null)
    {
        var fs = Helpers.CreateFileSystem();
        var facade =
            FileSystemPersistenceFacade.Initialize(fs.DataDirectory, Logger.None, snapshotOptions: snapshotOptions);
        var logEntries = ( tailEntries ?? Array.Empty<LogEntry>(),
                           segmentEntries ?? Array.Empty<IReadOnlyList<LogEntry>>() );
        facade.SetupTest(logEntries: logEntries,
            snapshotData: snapshotData,
            commitIndex: commitIndex,
            startLsn: startLsn,
            metadata: ( initialTerm ?? MetadataFile.DefaultTerm, votedFor ));
        var mockFs = new MockDataFileSystem(fs.Log, fs.Metadata, fs.Snapshot, fs.TemporaryDirectory, fs.DataDirectory);

        return ( facade, mockFs );
    }

    private static readonly LogEntryEqualityComparer Comparer = new();

    [Fact]
    public void Append__СПустымЛогом__НеДолженКоммититьЗапись()
    {
        var (facade, _) = CreateFacade();
        var oldCommitIndex = facade.CommitIndex;
        var entry = new LogEntry(new Term(2), Array.Empty<byte>());

        facade.Append(entry);

        var actualCommitIndex = facade.CommitIndex;
        Assert.Equal(oldCommitIndex, actualCommitIndex);
    }

    [Fact]
    public void Append__КогдаЕстьСнапшот__ДолженВернутьПравильныйНовыйИндекс()
    {
        var (facade, _) = CreateFacade();
        const int lastSnapshotIndex = 10;
        const int logSize = 20;
        facade.SetupTest(logEntries: Enumerable.Range(0, logSize)
                                               .Select(_ => RandomDataEntry(2))
                                               .ToArray(),
            snapshotData: ( new Term(2), lastSnapshotIndex, new StubSnapshot(Array.Empty<byte>()) ));
        var expected = new Lsn(logSize);

        var actual = facade.Append(new LogEntry(new Term(2), Array.Empty<byte>()));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Append__КогдаЛогПустИНичегоНеЗакоммичено__ДолженНеДолженКоммититьЗапись()
    {
        var (facade, _) = CreateFacade();
        var entry = new LogEntry(2, Array.Empty<byte>());
        var expectedCommitIndex = Lsn.Tomb;

        facade.Append(entry);

        Assert.Equal(expectedCommitIndex, facade.CommitIndex);
    }

    [Fact]
    public void Append__КогдаЛогПуст__ДолженВернутьАктуальнуюИнформациюОЗаписанномЭлементе()
    {
        var entry = new LogEntry(DefaultTerm, new byte[] {123, 4, 56});
        var (facade, _) = CreateFacade();

        // Индексирование начинается с 0
        var expected = new Lsn(0);

        var actual = facade.Append(entry);

        Assert.Equal(actual, expected);
    }

    [Fact]
    public void Append__КогдаЕстьНезакоммиченныеЭлементыИНетСнапшота__ДолженВернутьПравильныйИндекс()
    {
        var (facade, _) = CreateFacade(2);
        var logEntries = new List<LogEntry>()
        {
            Entry(1, "asdf"),    // 0
            Entry(2, "argdnnn"), // 1
        };
        facade.Log.SetupLogTest(logEntries);
        facade.SetCommitTest(0);
        var expected = new Lsn(2);
        var toAppend = Entry(3, "hhhhhhh");

        var actual = facade.Append(toAppend);

        Assert.Equal(expected, actual);
        Assert.Equal(expected, facade.LastEntry.Index);
    }

    [Fact]
    public void Append__КогдаЕстьНезакоммиченныеЭлементыИНетСнапшота__ДолженОбновитьИнформациюОПоследнемЭлементе()
    {
        var (facade, _) = CreateFacade(2);
        var logEntries = new List<LogEntry>()
        {
            Entry(1, "asdf"),    // 0
            Entry(2, "argdnnn"), // 1
        };
        facade.SetupTest(logEntries);
        facade.SetCommitTest(0);
        var expected = new LogEntryInfo(3, 2);
        var toAppend = Entry(3, "hhhhhhh");

        facade.Append(toAppend);

        Assert.Equal(expected, facade.LastEntry);
    }

    private static LogEntry Entry(Term term, params byte[] data) => new(term, data);

    private static LogEntry Entry(Term term, string data = "") =>
        new(term, Encoding.UTF8.GetBytes(data));

    [Fact]
    public void Append__КогдаЛогНеПустойИНетСнапшота__ДолженВернутьЗаписьСПравильнымИндексом()
    {
        var (facade, _) = CreateFacade(2);
        facade.Log.SetupLogTest(tailEntries: new[]
        {
            Entry(1, 99, 76, 33), // 0
            Entry(1, 9),          // 1
            Entry(2, 94, 22, 48)  // 2
        });

        // Должен вернуть индекс 3
        var entry = Entry(2, "data");
        var expected = new Lsn(3);

        var actual = facade.Append(entry);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Append__КогдаЛогНеПустИСнапшотЕсть__ДолженВернутьЗаписьСПравильнымИндексом()
    {
        var (facade, _) = CreateFacade(2);
        const int logSize = 30;
        facade.SetupTest(logEntries: Enumerable.Range(0, logSize).Select(i => Entry(1, i.ToString())).ToArray(),
            snapshotData: ( 1, 10, new StubSnapshot(Array.Empty<byte>()) ));
        var entry = Entry(2, "data");
        var expected = new Lsn(logSize);

        var actual = facade.Append(entry);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void
        Append__КогдаВЛогеЕстьЗакоммиченныеИНеЗакоммиченныеЗаписи__ДолженВернутьПравильныйИндексДобавленнойЗаписи()
    {
        var (facade, _) = CreateFacade(3);
        facade.Log.SetupLogTest(tailEntries: new[]
        {
            Entry(1, "adfasfas"),    // 0
            Entry(2, "aaaa"),        // 1
            Entry(2, "aegqer89987"), // 2
            Entry(3, "asdf"),        // 3 - не закоммичен
        });
        facade.SetCommitTest(2);
        var entry = Entry(4, "data");
        var expected = new Lsn(4);

        var actual = facade.Append(entry);

        Assert.Equal(expected, actual);
    }

    // TODO: тесты на то, что LastEntry обновляется
    private static LogEntry RandomDataEntry(Term term)
    {
        var buffer = new byte[Random.Shared.Next(0, 32)];
        Random.Shared.NextBytes(buffer);
        return new LogEntry(term, buffer);
    }

    private static LogEntry RandomDataEntry(int term) => RandomDataEntry(new Term(term));

    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 0)]
    [InlineData(2, 1)]
    [InlineData(3, 1)]
    [InlineData(10, 6)]
    [InlineData(1230, 1229)]
    public void Commit__КогдаНичегоНеБылоЗакоммичено__ДолженОбновитьИндексКоммита(
        int logSize,
        int commitIndex)
    {
        var logEntries = Enumerable.Range(1, logSize)
                                   .Select(_ => Entry(1, "hello"))
                                   .ToArray();
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries);

        facade.Commit(commitIndex);

        Assert.Equal(commitIndex, facade.CommitIndex);
    }

    [Theory]
    [InlineData(2, 0, 1)]
    [InlineData(3, 0, 2)]
    [InlineData(3, 0, 1)]
    [InlineData(10, 6, 9)]
    [InlineData(1230, 0, 1229)]
    public void Commit__КогдаЧастьЗаписейЗакоммичена__ДолженОбновитьИндексКоммита(
        int logSize,
        int oldCommitIndex,
        int commitIndex)
    {
        var logEntries = Enumerable.Range(1, logSize)
                                   .Select(_ => Entry(1, "hello"))
                                   .ToArray();
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries);
        facade.SetCommitTest(oldCommitIndex);

        facade.Commit(commitIndex);

        Assert.Equal(commitIndex, facade.CommitIndex);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    public void InsertRange__КогдаСнапшотаНетИНетЗаписейИИндекс0__ДолженДобавитьЗаписиВКонец(int elementsCount)
    {
        var (facade, _) = CreateFacade(elementsCount + 1);
        var entries = Enumerable.Range(1, elementsCount)
                                .Select(RandomDataEntry)
                                .ToArray();

        facade.InsertRange(entries, 0);

        var actual = facade.Log.GetAllEntriesTest();
        Assert.Equal(entries, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 2, 2)]
    [InlineData(1, 1, 1)]
    [InlineData(2, 1, 2)]
    [InlineData(10, 1, 10)]
    [InlineData(10, 10, 19)]
    public void InsertRange__КогдаЗаписиБылиДобавленыВКонец__ДолженОбновитьLastEntry(
        int logSize,
        int insertCount,
        int expectedLsn)
    {
        var (facade, _) = CreateFacade(2);
        var existingEntries = Enumerable.Range(1, logSize)
                                        .Select(i => Entry(1, i.ToString()))
                                        .ToArray();
        facade.SetupTest(existingEntries);
        var toInsert = Enumerable.Range(logSize, insertCount)
                                 .Select(i => Entry(2, i.ToString()))
                                 .ToArray();
        var expected = new LogEntryInfo(2, expectedLsn);

        facade.InsertRange(toInsert, logSize);

        Assert.Equal(expected, facade.LastEntry);
    }

    [Theory]
    [InlineData(10, 1, 7, 7)]
    [InlineData(10, 2, 7, 8)]
    [InlineData(10, 3, 7, 9)]
    [InlineData(10, 4, 7, 10)]
    [InlineData(10, 5, 7, 11)]
    [InlineData(10, 6, 7, 12)]
    public void InsertRange__КогдаЧастьДанныхБылаПерезаписана__ДолженОбновитьLastEntry(
        int logSize,
        int insertCount,
        int insertIndex,
        int expectedLsn)
    {
        var (facade, _) = CreateFacade(2);
        var existingEntries = Enumerable.Range(1, logSize)
                                        .Select(i => Entry(1, i.ToString()))
                                        .ToArray();
        facade.SetupTest(existingEntries);
        var toInsert = Enumerable.Range(logSize, insertCount)
                                 .Select(i => Entry(2, i.ToString()))
                                 .ToArray();
        var expected = new LogEntryInfo(2, expectedLsn);

        facade.InsertRange(toInsert, insertIndex);

        Assert.Equal(expected, facade.LastEntry);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 5)]
    [InlineData(2, 1)]
    [InlineData(2, 2)]
    [InlineData(2, 4)]
    [InlineData(3, 3)]
    [InlineData(5, 1)]
    [InlineData(5, 5)]
    public void InsertRange__КогдаЛогНеПустИИндексРавенСледующемуПослеПоследнего__ДолженДобавитьЗаписиВКонец(
        int logSize,
        int insertCount)
    {
        var logEntries = Enumerable.Range(1, logSize)
                                   .Select(RandomDataEntry)
                                   .ToList();
        var insert = Enumerable.Range(logSize + 1, insertCount)
                               .Select(RandomDataEntry)
                               .ToList();
        var expected = logEntries.Concat(insert)
                                 .ToList();
        var (facade, _) = CreateFacade(logSize + insertCount + 1);
        facade.SetupTest(logEntries);

        facade.InsertRange(insert, logSize);

        var actual = facade.Log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(2, 1, 1)]
    [InlineData(5, 5, 3)]
    [InlineData(5, 5, 2)]
    [InlineData(5, 5, 1)]
    [InlineData(5, 2, 1)]
    [InlineData(5, 1, 1)]
    [InlineData(5, 4, 1)]
    [InlineData(1, 4, 0)]
    [InlineData(3, 4, 0)]
    [InlineData(4, 4, 0)]
    [InlineData(6, 4, 0)]
    [InlineData(6, 4, 5)]
    [InlineData(10, 4, 5)]
    public void InsertRange__КогдаЛогНеПустИИндексВнутриЛога__ДолженПерезаписатьЛог(
        int logSize,
        int toInsertCount,
        int insertIndex)
    {
        var logEntries = Enumerable.Range(1, logSize)
                                   .Select(EmptyEntry)
                                   .ToList();
        var toInsert = Enumerable.Range(logSize + 1, toInsertCount)
                                 .Select(EmptyEntry)
                                 .ToList();
        var expected = logEntries.Take(insertIndex)
                                 .Concat(toInsert)
                                 .ToList();
        var (facade, _) = CreateFacade(logSize + toInsertCount + 1);
        facade.Log.SetupLogTest(logEntries);

        facade.InsertRange(toInsert, insertIndex);

        var actual = facade.Log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void SaveSnapshot__КогдаФайлаСнапшотаНеБыло__ДолженСоздатьНовыйФайлСнапшота()
    {
        var logEntries = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(4), // 1
            RandomDataEntry(5), // 2
            RandomDataEntry(6), // 3
        };
        var (facade, fs) = CreateFacade();
        var expectedLastEntry = new LogEntryInfo(new Term(6), 3);
        var snapshotData = new byte[] {1, 2, 3};
        facade.SetupTest(logEntries);

        facade.SaveSnapshot(new StubSnapshot(snapshotData), new LogEntryInfo(6, 3));

        var (actualIndex, actualTerm, actualData) = facade.Snapshot.ReadAllDataTest();
        Assert.True(fs.SnapshotFile.Exists);
        Assert.Equal(expectedLastEntry.Index, actualIndex);
        Assert.Equal(expectedLastEntry.Term, actualTerm);
        Assert.Equal(snapshotData, actualData);
        Assert.Equal(expectedLastEntry, facade.Snapshot.LastApplied);
    }

    [Fact]
    public void SaveSnapshot__КогдаФайлСнапшотаСуществовалПустой__ДолженПерезаписатьСтарыйФайл()
    {
        var logEntries = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(3), // 2
            RandomDataEntry(4), // 3
            RandomDataEntry(5), // 4
        };
        var data = new byte[128];
        Random.Shared.NextBytes(data);

        var (facade, _) = CreateFacade();
        facade.Log.SetupLogTest(logEntries);

        var snapshotEntry = new LogEntryInfo(new Term(3), 4);
        facade.SaveSnapshot(new StubSnapshot(data), snapshotEntry);

        var (actualIndex, actualTerm, actualData) = facade.Snapshot.ReadAllDataTest();
        Assert.Equal(snapshotEntry.Index, actualIndex);
        Assert.Equal(snapshotEntry.Term, actualTerm);
        Assert.Equal(data, actualData);
        Assert.Equal(snapshotEntry, facade.Snapshot.LastApplied);
    }

    [Fact]
    public void CreateSnapshot__КогдаСнапшотаНеБылоИЛогПуст__ДолженСоздатьНовыйСнапшот()
    {
        var (facade, fs) = CreateFacade();
        var lastSnapshotEntry = new LogEntryInfo(new Term(7), 10);
        var snapshotData = RandomBytes(100);

        var writer = facade.CreateSnapshot(lastSnapshotEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var (actualLastIndex, actualLastTerm, actualData) = facade.Snapshot.ReadAllDataTest();
        Assert.True(fs.SnapshotFile.Exists);
        Assert.Equal(snapshotData, actualData);
        Assert.Equal(lastSnapshotEntry.Term, actualLastTerm);
        Assert.Equal(lastSnapshotEntry.Index, actualLastIndex);
    }

    [Fact]
    public void
        CreateSnapshot__КогдаИндексСнапшотаУказываетНаЗаписьВСегментеВСередине__ДолженУдалитьИзЛогаПокрываемыеСегменты()
    {
        const int segmentsCount = 10;
        const int segmentSize = 100;

        var segmentEntries = Enumerable.Range(0, segmentsCount)
                                       .Select(i => Enumerable.Range(0, segmentSize)
                                                              .Select(s => Entry(1, ( s + i * segmentSize ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(0, 10)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();

        var (facade, fs) = CreateFacade(segmentEntries: segmentEntries, tailEntries: tailEntries);
        var middleSegmentIndex = segmentSize * 4 + 1;
        var lastSnapshotEntry = new LogEntryInfo(1, middleSegmentIndex);
        var snapshotData = RandomBytes(100);
        var expectedSegmentsCount = 6 + 1; // Удаляются все до 5-ого (осталось 6) + хвост 

        var writer = facade.CreateSnapshot(lastSnapshotEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        Assert.Equal(expectedSegmentsCount, facade.Log.GetSegmentsCountTest());

        var (actualLastIndex, actualLastTerm, actualData) = facade.Snapshot.ReadAllDataTest();
        Assert.True(fs.SnapshotFile.Exists);
        Assert.Equal(snapshotData, actualData);
        Assert.Equal(lastSnapshotEntry.Term, actualLastTerm);
        Assert.Equal(lastSnapshotEntry.Index, actualLastIndex);
    }

    [Fact]
    public void CreateSnapshot__КогдаИндексУказывалНаИндексВнутриХвоста__ДолженУдалитьВсеЗакрытыеСегменты()
    {
        const int segmentsCount = 10;
        const int segmentSize = 100;

        var segmentEntries = Enumerable.Range(0, segmentsCount)
                                       .Select(i => Enumerable.Range(0, segmentSize)
                                                              .Select(s => Entry(1, ( s + i * segmentSize ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(0, 10)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();

        var (facade, fs) = CreateFacade(segmentEntries: segmentEntries, tailEntries: tailEntries);
        var tailIndex = segmentSize * segmentsCount + 1;
        var lastSnapshotEntry = new LogEntryInfo(1, tailIndex);
        var snapshotData = RandomBytes(100);

        var writer = facade.CreateSnapshot(lastSnapshotEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        Assert.Equal(1, facade.Log.GetSegmentsCountTest());

        var (actualLastIndex, actualLastTerm, actualData) = facade.Snapshot.ReadAllDataTest();
        Assert.True(fs.SnapshotFile.Exists);
        Assert.Equal(snapshotData, actualData);
        Assert.Equal(lastSnapshotEntry.Term, actualLastTerm);
        Assert.Equal(lastSnapshotEntry.Index, actualLastIndex);
    }

    [Fact]
    public void CreateSnapshot__КогдаИндексБольшеПоследнего__ДолженНачатьНовыйЛог()
    {
        const int segmentsCount = 10;
        const int segmentSize = 100;

        var segmentEntries = Enumerable.Range(0, segmentsCount)
                                       .Select(i => Enumerable.Range(0, segmentSize)
                                                              .Select(s => Entry(1, ( s + i * segmentSize ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tailEntries = Enumerable.Range(0, 10)
                                    .Select(i => Entry(1, i.ToString()))
                                    .ToArray();

        var (facade, fs) = CreateFacade(segmentEntries: segmentEntries, tailEntries: tailEntries);
        var middleSegmentIndex = segmentSize * segmentsCount + tailEntries.Length + 1;
        var lastSnapshotEntry = new LogEntryInfo(1, middleSegmentIndex);
        var snapshotData = RandomBytes(100);

        var writer = facade.CreateSnapshot(lastSnapshotEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        Assert.Equal(1, facade.Log.GetSegmentsCountTest());
        Assert.Empty(facade.Log.ReadAllTest());
        Assert.Equal(lastSnapshotEntry.Index, facade.Log.StartIndex);

        var (actualLastIndex, actualLastTerm, actualData) = facade.Snapshot.ReadAllDataTest();
        Assert.True(fs.SnapshotFile.Exists);
        Assert.Equal(snapshotData, actualData);
        Assert.Equal(lastSnapshotEntry.Term, actualLastTerm);
        Assert.Equal(lastSnapshotEntry.Index, actualLastIndex);
    }

    [Fact]
    public void LastEntry__КогдаЛогПуст__ДолженВернутьTomb()
    {
        var (facade, _) = CreateFacade();

        var actual = facade.LastEntry;

        Assert.Equal(LogEntryInfo.Tomb, actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    public void LastEntry__КогдаВЛогеЕстьЭлементы__ДолженВернутьПоследнийЭлементИзЛога(int logSize)
    {
        var (facade, _) = CreateFacade();
        var logEntries = Enumerable.Range(1, logSize).Select(i => Entry(i, i.ToString())).ToArray();
        var expected = new LogEntryInfo(logEntries[^1].Term, logSize - 1);
        facade.SetupTest(logEntries);

        var actual = facade.LastEntry;

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    public void LastEntry__КогдаЛогПустИНачинаетсяНеС0ИЕстьСнапшот__ДолженВернутьЗаписьИзСнапшота(int snapshotIndex)
    {
        var snapshotTerm = new Term(2);
        var (facade, _) = CreateFacade(startLsn: snapshotIndex + 1,
            tailEntries: Array.Empty<LogEntry>(),
            segmentEntries: Array.Empty<IReadOnlyList<LogEntry>>(),
            snapshotData: ( snapshotTerm, snapshotIndex, new StubSnapshot([1, 2, 3]) ));
        var expected = new LogEntryInfo(snapshotTerm, snapshotIndex);

        var actual = facade.LastEntry;

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0, 3)]
    [InlineData(1, 3)]
    [InlineData(9, 100)]
    [InlineData(77, 100)]
    [InlineData(99, 100)]
    public void LastEntry__КогдаЛогНеПустИСнапшотЕсть__ДолженВернутьЗаписьИзЛога(int snapshotIndex, int logSize)
    {
        var logEntries = Enumerable.Range(0, logSize)
                                   .Select(e => Entry(1, e.ToString()))
                                   .ToArray();
        var (facade, _) = CreateFacade(tailEntries: logEntries,
            snapshotData: ( 1, snapshotIndex, new StubSnapshot([1, 2, 3, 4, 5, 5]) ));
        var expected = new LogEntryInfo(logEntries[^1].Term, logSize - 1);

        var actual = facade.LastEntry;

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void CreateSnapshot__КогдаФайлСнапшотаСуществовалНеПустой__ДолженСохранитьДанныеСнапшота()
    {
        var (facade, _) = CreateFacade();
        facade.Snapshot.SetupSnapshotTest(new Term(4), 3, new StubSnapshot(new byte[] {1, 2, 3}));
        var lastLogEntry = new LogEntryInfo(new Term(7), 10);
        var snapshotData = RandomBytes(123);

        var writer = facade.CreateSnapshot(lastLogEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var (actualLastIndex, actualLastTerm, actualData) = facade.Snapshot.ReadAllDataTest();

        Assert.Equal(lastLogEntry.Index, actualLastIndex);
        Assert.Equal(lastLogEntry.Term, actualLastTerm);
        Assert.Equal(snapshotData, actualData);
        Assert.Equal(lastLogEntry, facade.Snapshot.LastApplied);
    }

    [Fact]
    public void
        CreateSnapshot__КогдаСнапшотаНетИИндексВСнапшотеМеньшеКоличестваЭлементовВЛоге__ДолженИзменятьЛог()
    {
        var (facade, _) = CreateFacade();
        var snapshotData = RandomBytes(123);
        var logEntries = Enumerable.Range(1, 10)
                                   .Select(i => Entry(i, i.ToString()))
                                   .ToArray();
        facade.Log.SetupLogTest(logEntries);
        var lastSnapshotEntry = new LogEntryInfo(new Term(5), 4);

        var writer = facade.CreateSnapshot(lastSnapshotEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var actual = facade.Log.ReadAllTest();
        Assert.Equal(logEntries, actual, Comparer);
    }

    [Fact]
    public void CreateSnapshot__КогдаФайлаСнапшотаНеСуществовало__ДолженСохранитьДанныеСнапшота()
    {
        // Должен удалить предшествующие записи в логе
        var (facade, _) = CreateFacade();

        var lastLogEntry = new LogEntryInfo(1, 10);
        var snapshotData = RandomBytes(123);

        var stubSnapshot = new StubSnapshot(snapshotData);
        var writer = facade.CreateSnapshot(lastLogEntry);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var (actualLastIndex, actualLastTerm, actualData) = facade.Snapshot.ReadAllDataTest();

        Assert.Equal(lastLogEntry.Index, actualLastIndex);
        Assert.Equal(lastLogEntry.Term, actualLastTerm);
        Assert.Equal(snapshotData, actualData);
        Assert.Equal(lastLogEntry, facade.Snapshot.LastApplied);
    }

    [Theory]
    [InlineData(0, 1, 123, 0)]
    [InlineData(0, 1, 123, 32)]
    [InlineData(0, 1, 123, 64)]
    [InlineData(0, 1, 123, 80)]
    [InlineData(0, 1, 123, 186)]
    public void CreateSnapshot__КогдаИндексВСнапшотеРавенЗакоммиченному__ДолженСоздатьНовыйСнапшот(
        int startIndex,
        int segmentsCount,
        int tailSize,
        int snapshotIndex)
    {
        const int segmentSize = 64;

        var segmentEntries = Enumerable.Range(0, segmentsCount)
                                       .Select(s => Enumerable.Range(1, segmentSize)
                                                              .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                              .ToArray())
                                       .ToArray();
        var tail = Enumerable.Range(0, tailSize)
                             .Select(e => Entry(1, e.ToString()))
                             .ToArray();
        byte[] snapshotChunk = [1, 2, 3, 4, 5];
        var commitIndex = snapshotIndex;
        var (facade, _) = CreateFacade(startLsn: startIndex,
            tailEntries: tail,
            segmentEntries: segmentEntries,
            commitIndex: commitIndex);
        var lastIncludedEntry = new LogEntryInfo(1, snapshotIndex);

        var si = facade.CreateSnapshot(lastIncludedEntry);
        si.InstallChunk(snapshotChunk, CancellationToken.None);
        si.Commit();

        var success = facade.Snapshot.TryGetSnapshot(out var actualSnapshot, out var actualLastApplied);
        Assert.True(success);
        var chunks = new List<byte>();
        foreach (var memory in actualSnapshot.GetAllChunks())
        {
            chunks.AddRange(memory.Span);
        }

        Assert.Equal(snapshotChunk, chunks);
        Assert.Equal(lastIncludedEntry, actualLastApplied);
    }

    private static byte[] RandomBytes(int size)
    {
        var bytes = new byte[size];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }

    [Fact]
    public void SaveSnapshot__КогдаФайлСнапшотаСуществовал__ДолженПерезаписатьСтарыйФайл()
    {
        var newSnapshotData = new byte[128];
        Random.Shared.NextBytes(newSnapshotData);

        var (facade, _) = CreateFacade();
        // Cуществует старый файл снапшота 
        var oldData = new byte[164];
        Random.Shared.NextBytes(oldData);
        facade.Snapshot.SetupSnapshotTest(new Term(2), 3, new StubSnapshot(oldData));

        // У нас в логе 4 команды, причем применены все
        var lastTerm = new Term(5);
        var logEntries = new[]
        {
            RandomDataEntry(3),        // 4
            RandomDataEntry(3),        // 5
            RandomDataEntry(4),        // 6
            RandomDataEntry(lastTerm), // 7
        };
        facade.Log.SetupLogTest(logEntries);
        // Все команды применены из лога

        var snapshotLastEntry = new LogEntryInfo(4, 7);
        facade.SaveSnapshot(new StubSnapshot(newSnapshotData), snapshotLastEntry);

        var (actualIndex, actualTerm, actualData) = facade.Snapshot.ReadAllDataTest();
        Assert.Equal(snapshotLastEntry.Index, actualIndex);
        Assert.Equal(snapshotLastEntry.Term, actualTerm);
        Assert.Equal(newSnapshotData, actualData);
        Assert.Equal(snapshotLastEntry, facade.Snapshot.LastApplied);
    }

    [Theory]
    [InlineData(12)]
    [InlineData(13)]
    [InlineData(20)]
    [InlineData(100)]
    public void InsertRange__КогдаЕстьСнапшотИИндексПоследнийВЛоге__ДолженДобавитьЗаписиВКонец(int logSize)
    {
        var (facade, _) = CreateFacade();
        var snapshotLastIndex = 11;
        var logEntries = Enumerable.Range(0, logSize)
                                   .Select(_ => RandomDataEntry(3))
                                   .ToArray();
        facade.SetupTest(logEntries: logEntries,
            snapshotData: ( new Term(3), snapshotLastIndex, new StubSnapshot(Array.Empty<byte>()) ));

        var toInsert = new[] {RandomDataEntry(3), RandomDataEntry(3),};
        var expected = logEntries.Concat(toInsert).ToArray();

        facade.InsertRange(toInsert, logSize);

        Assert.Equal(expected, facade.Log.ReadAllTest(), Comparer);
    }

    [Fact]
    public void SaveSnapshot__КогдаСнапшотНеСуществовал__ДолженОставитьФайлВКорректномСостоянии()
    {
        var (facade, fs) = CreateFacade();
        var expected = "hello, world!dfsddfd1284923yt0q984vt"u8.ToArray();

        facade.SaveSnapshot(new StubSnapshot(expected), new LogEntryInfo(1, 0));

        var ex = Record.Exception(() => SnapshotFile.Initialize(fs.DataDirectory));
        Assert.Null(ex);
    }

    [Fact]
    public void PrefixMatch__КогдаЛогПустИПереданTomb__ДолженВернутьTrue()
    {
        var (facade, _) = CreateFacade();

        var match = facade.PrefixMatch(LogEntryInfo.Tomb);

        Assert.True(match);
    }

    [Fact]
    public void PrefixMatch__КогдаЛогНеПустИПереданTomb__ДолженВернутьTrue()
    {
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries: new[] {Entry(1, "asdf"), Entry(2, "vdfgra"),});

        var match = facade.PrefixMatch(LogEntryInfo.Tomb);

        Assert.True(match);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public void PrefixMatch__КогдаПередаютсяДанныеОПоследнейЗаписиВЭтомЛоге__ДолженВернутьTrue(int logSize)
    {
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries: Enumerable.Range(1, logSize).Select(i => Entry(i, i.ToString())).ToArray());
        var lastEntryInfo = new LogEntryInfo(logSize, logSize - 1);

        var match = facade.PrefixMatch(lastEntryInfo);

        Assert.True(match);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public void PrefixMatch__КогдаПередаютсяДанныеОПредпоследнейЗаписиВЭтомЛоге__ДолженВернутьTrue(int logSize)
    {
        var (facade, _) = CreateFacade();
        facade.SetupTest(Enumerable.Range(1, logSize).Select(i => Entry(i, i.ToString())).ToArray());
        var lastEntryInfo = new LogEntryInfo(logSize - 1, logSize - 2);

        var match = facade.PrefixMatch(lastEntryInfo);

        Assert.True(match);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(20)]
    public void PrefixMatch__КогдаИндексРавенПоследнемуНоТермБольше__ДолженВернутьFalse(int logSize)
    {
        var (facade, _) = CreateFacade();
        facade.SetupTest(Enumerable.Range(1, logSize).Select(i => Entry(i, i.ToString())).ToArray());
        var lastEntryInfo = new LogEntryInfo(logSize + 1, logSize - 1);

        var match = facade.PrefixMatch(lastEntryInfo);

        Assert.False(match);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 0)]
    [InlineData(4, 2)]
    [InlineData(5, 2)]
    public void PrefixMatch__КогдаИндексВнутриЛогаНоТермБольше__ДолженВернутьFalse(int logSize, int index)
    {
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries: Enumerable.Range(1, logSize).Select(i => Entry(i, i.ToString())).ToArray());
        var lastEntryInfo = new LogEntryInfo(index + 2, index);

        var match = facade.PrefixMatch(lastEntryInfo);

        Assert.False(match);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public void PrefixMatch__КогдаИндексРавенПоследнемуВЛогеНоТермМеньше__ДолженВернутьFalse(int logSize)
    {
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries: Enumerable.Range(1, logSize).Select(i => Entry(i, i.ToString())).ToArray());
        var lastEntryInfo = new LogEntryInfo(logSize - 1, logSize);

        var match = facade.PrefixMatch(lastEntryInfo);

        Assert.False(match);
    }

    [Fact]
    public void PrefixMatch__КогдаЕстьСнапшотИДанныеРавныЗаписиИзСнапшота__ДолженВернутьTrue()
    {
        var snapshotLastEntry = new LogEntryInfo(30, 29);
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries: Enumerable.Range(1, ( int ) ( snapshotLastEntry.Index + 10 ))
                                               .Select(i => Entry(i, i.ToString()))
                                               .ToArray(),
            snapshotData: ( snapshotLastEntry.Term, snapshotLastEntry.Index, new StubSnapshot(Array.Empty<byte>()) ));

        var match = facade.PrefixMatch(snapshotLastEntry);

        Assert.True(match);
    }

    [Fact]
    public void PrefixMatch__КогдаЕстьСнапшотИДанныеОЗаписиРавныПоследнейВЛоге__ДолженВернутьTrue()
    {
        var snapshotLastEntry = new LogEntryInfo(30, 29);
        var (facade, _) = CreateFacade();
        var logEntries = Enumerable.Range(1, ( int ) ( snapshotLastEntry.Index + 10 ))
                                   .Select(i => Entry(i, i.ToString()))
                                   .ToArray();
        facade.SetupTest(logEntries: logEntries.ToArray(),
            snapshotData: ( snapshotLastEntry.Term, snapshotLastEntry.Index, new StubSnapshot(Array.Empty<byte>()) ));
        var logEntryInfo = new LogEntryInfo(logEntries[^1].Term, logEntries.Length - 1);

        var match = facade.PrefixMatch(logEntryInfo);

        Assert.True(match);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(0, 1)]
    [InlineData(10, 10)]
    [InlineData(10, 11)]
    [InlineData(10, 20)]
    [InlineData(1000, 1000)]
    public void PrefixMatch__КогдаИндексБольшеРазмераЛога__ДолженВернутьFalse(int logSize, int index)
    {
        var (facade, _) = CreateFacade();
        facade.SetupTest(Enumerable.Range(1, logSize).Select(i => Entry(i, i.ToString())).ToArray());
        var lastEntryInfo = new LogEntryInfo(logSize + 1, index);

        var match = facade.PrefixMatch(lastEntryInfo);

        Assert.False(match);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(56734)]
    public void PrefixMatch__КогдаПередаетсяИндексМеньшеНачального__ДолженВернутьFalse(int startIndex)
    {
        var (facade, _) = CreateFacade(startLsn: startIndex, tailEntries: new[] {Entry(1, "asdf")});

        var match = facade.PrefixMatch(new LogEntryInfo(1, startIndex - 1));

        Assert.False(match);
    }

    [Fact]
    public void IsUpToDate__КогдаПереданаПоследняяЗаписьИзЛога__ДолженВернутьTrue()
    {
        var (facade, _) = CreateFacade();
        var entries = new[]
        {
            Entry(1, "helo"),      // 0
            Entry(2, "aasaadsdf"), // 1
            Entry(2, "45uwhr"),    // 2
        };
        facade.SetupTest(entries);
        var last = new LogEntryInfo(2, 2);

        var actual = facade.IsUpToDate(last);

        Assert.True(actual);
    }

    [Fact]
    public void IsUpToDate__КогдаПереданаПредпоследняяЗаписьИзЛога__ДолженВернутьFalse()
    {
        var (facade, _) = CreateFacade();
        var entries = new[]
        {
            Entry(1, "helo"),      // 0
            Entry(2, "aasaadsdf"), // 1
            Entry(2, "45uwhr"),    // 2
        };
        facade.SetupTest(entries);
        var last = new LogEntryInfo(2, 1);

        var actual = facade.IsUpToDate(last);

        Assert.False(actual);
    }

    [Fact]
    public void IsUpToDate__КогдаПереданTombИЛогПуст__ДолженВернутьTrue()
    {
        var (facade, _) = CreateFacade();

        var actual = facade.IsUpToDate(LogEntryInfo.Tomb);

        Assert.True(actual);
    }

    [Fact]
    public void IsUpToDate__КогдаПереданаЗаписьСПоследнимИндексомНоБольшимТермом__ДолженВернутьTrue()
    {
        var (facade, _) = CreateFacade();
        var entries = new[]
        {
            Entry(1, "helo"),      // 0
            Entry(2, "aasaadsdf"), // 1
            Entry(2, "45uwhr"),    // 2
        };
        facade.SetupTest(entries);
        var last = new LogEntryInfo(3, 2);

        var actual = facade.IsUpToDate(last);

        Assert.True(actual);
    }

    [Fact]
    public void IsUpToDate__КогдаПереданаЗаписьСМеньшимИндексомИБольшимТермом__ДолженВернутьTrue()
    {
        var (facade, _) = CreateFacade();
        var entries = new[]
        {
            Entry(1, "helo"),      // 0
            Entry(2, "aasaadsdf"), // 1
            Entry(2, "45uwhr"),    // 2
        };
        facade.SetupTest(entries);
        var last = new LogEntryInfo(3, 1);

        var actual = facade.IsUpToDate(last);

        Assert.True(actual);
    }

    [Fact]
    public void IsUpToDate__КогдаПереданаЗаписьСБольшимИндексомИМеньшимТермом__ДолженВернутьFalse()
    {
        var (facade, _) = CreateFacade();
        var entries = new[]
        {
            Entry(1, "helo"),      // 0
            Entry(2, "aasaadsdf"), // 1
            Entry(2, "45uwhr"),    // 2
        };
        facade.SetupTest(entries);
        var last = new LogEntryInfo(1, 3);

        var actual = facade.IsUpToDate(last);

        Assert.False(actual);
    }

    [Fact]
    public void IsUpToDate__КогдаПереданTombИЛогНеПуст__ДолженВернутьFalse()
    {
        var (facade, _) = CreateFacade();
        var entries = new[]
        {
            Entry(1, "helo"),      // 0
            Entry(2, "aasaadsdf"), // 1
            Entry(2, "45uwhr"),    // 2
        };
        facade.SetupTest(entries);

        var actual = facade.IsUpToDate(LogEntryInfo.Tomb);

        Assert.False(actual);
    }

    [Fact]
    public void IsUpToDate__КогдаПереданНеTombИЛогПуст__ДолженВернутьTrue()
    {
        var (facade, _) = CreateFacade();
        var notTomb = new LogEntryInfo(2, 2);

        var actual = facade.IsUpToDate(notTomb);

        Assert.True(actual);
    }

    private class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[]? x, byte[]? y)
        {
            return x != null && y != null && x.SequenceEqual(y);
        }

        public int GetHashCode(byte[] obj)
        {
            return obj.GetHashCode();
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(100)]
    public void ReadDeltaFromPreviousSnapshot__КогдаСнапшотаНет__ДолженВернутьВсеЗаписиИзЛога(int logSize)
    {
        var logEntries = Enumerable.Range(1, logSize)
                                   .Select(i => Entry(i, i.ToString()))
                                   .ToArray();
        var (facade, _) = CreateFacade(tailEntries: logEntries);

        var actual = facade.ReadDeltaFromPreviousSnapshot();

        Assert.Equal(logEntries.Select(e => e.Data), actual, new ByteArrayEqualityComparer());
    }

    [Fact]
    public void ReadDeltaFromPreviousSnapshot__КогдаСнапшотаНетИЛогПуст__ДолженВернутьПустойМассив()
    {
        var (facade, _) = CreateFacade(tailEntries: null, segmentEntries: null, snapshotData: null);

        var actual = facade.ReadDeltaFromPreviousSnapshot();

        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    public void
        ReadDeltaFromPreviousSnapshot__КогдаСнапшотЕстьИИндексПоследнейЗаписиРавенИндексуСнапшота__ДолженВернутьПустойМассив(
        int snapshotIndex)
    {
        var tailEntries = Enumerable.Range(1, snapshotIndex + 1)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries: tailEntries,
            snapshotData: ( snapshotIndex + 1, snapshotIndex, new StubSnapshot([1, 2, 3]) ));

        var actual = facade.ReadDeltaFromPreviousSnapshot();

        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(1, 10)]
    [InlineData(23, 50)]
    [InlineData(48, 50)]
    public void
        ReadDeltaFromPreviousSnapshot__КогдаСнапшотЕстьИИндексПоследнейЗаписиБольшеИндексаСнапшота__ДолженВернутьВсеЗаписиПослеИндексаСнапшота(
        int snapshotIndex,
        int logSize)
    {
        var logEntries = Enumerable.Range(1, logSize)
                                   .Select(i => Entry(i, i.ToString()))
                                   .ToArray();
        var (facade, _) = CreateFacade(tailEntries: logEntries);
        facade.SetupTest(logEntries: logEntries,
            snapshotData: ( logEntries[snapshotIndex].Term, snapshotIndex, new StubSnapshot([1, 2, 3, 4]) ));
        var expected = logEntries.Skip(snapshotIndex + 1).Select(i => i.Data);

        var actual = facade.ReadDeltaFromPreviousSnapshot();

        Assert.Equal(expected, actual, new ByteArrayEqualityComparer());
    }

    [Fact]
    public void ReadCommittedDeltaFromPreviousSnapshot__КогдаСнапшотаНетИЛогПуст__ДолженВернутьПустойМассив()
    {
        var (facade, _) = CreateFacade();

        var actual = facade.ReadCommittedDeltaFromPreviousSnapshot();

        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    public void
        ReadCommittedDeltaFromPreviousSnapshot__КогдаСнапшотаНетИЛогНеПустИИндексКоммитаМеньшеНачального__ДолженВернутьПустойМассив(
        int startLsn)
    {
        var entries = new[] {Entry(1, "ss"), Entry(2, "fdfd"), Entry(2, "gggsdg"),};
        var (facade, _) = CreateFacade(tailEntries: entries, startLsn: startLsn);
        facade.SetCommitTest(startLsn - 1);

        var actual = facade.ReadCommittedDeltaFromPreviousSnapshot();

        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(0, 10, 0)]
    [InlineData(0, 10, 1)]
    [InlineData(0, 10, 7)]
    [InlineData(0, 10, 9)]
    [InlineData(10, 10, 10)]
    [InlineData(10, 10, 15)]
    [InlineData(10, 10, 19)]
    public void
        ReadCommittedDeltaFromPreviousSnapshot__КогдаСнапшотаНетИЛогНеПустИИндексКоммитаБольшеНачального__ДолженВернутьЗаписиДоУказанногоИндекса(
        int startLsn,
        int logSize,
        int commitIndex)
    {
        var entries = Enumerable.Range(0, logSize).Select(i => Entry(1, i.ToString())).ToArray();
        var (facade, _) = CreateFacade(startLsn: startLsn, tailEntries: entries);
        facade.SetCommitTest(commitIndex);
        var expected = entries.Take(commitIndex - startLsn + 1).Select(e => e.Data);

        var actual = facade.ReadCommittedDeltaFromPreviousSnapshot();

        Assert.Equal(expected, actual, new ByteArrayEqualityComparer());
    }

    [Theory]
    [InlineData(0, 0, 0, 10)]
    [InlineData(0, 0, 1, 10)]
    [InlineData(0, 0, 5, 10)]
    [InlineData(0, 0, 9, 10)]
    [InlineData(0, 4, 4, 10)]
    [InlineData(0, 4, 5, 10)]
    [InlineData(0, 4, 9, 10)]
    [InlineData(10, 10, 19, 10)]
    [InlineData(10, 11, 19, 10)]
    [InlineData(10, 18, 19, 10)]
    [InlineData(10, 12, 15, 10)]
    public void
        ReadCommittedDeltaFromPreviousSnapshot__КогдаСнапшотЕстьИЛогНеПустИИндексКоммитаБольшеНачального__ДолженВернутьЗаписиДоУказанногоИндекса(
        int startLsn,
        int snapshotIndex,
        int commitIndex,
        int logSize)
    {
        var entries = Enumerable.Range(0, logSize).Select(i => Entry(1, i.ToString())).ToArray();
        var (facade, _) = CreateFacade();
        facade.SetupTest(startLsn: startLsn,
            logEntries: entries,
            snapshotData: ( 1, snapshotIndex, new StubSnapshot([1, 2, 3, 99]) ),
            commitIndex: commitIndex);
        var expected = entries.Skip(snapshotIndex - startLsn + 1)
                              .Take(commitIndex - snapshotIndex)
                              .Select(e => e.Data);

        var actual = facade.ReadCommittedDeltaFromPreviousSnapshot();

        Assert.Equal(expected, actual, new ByteArrayEqualityComparer());
    }

    [Theory]
    [InlineData(1, 9, 10)]
    [InlineData(1, 10, 10)]
    [InlineData(1, 9, 20)]
    [InlineData(5, 9, 20)]
    [InlineData(5, 19, 20)]
    [InlineData(18, 18, 1)]
    [InlineData(18, 19, 2)]
    public void
        ReadCommittedDeltaFromPreviousSnapshot__КогдаИндексСнапшотаРавенПредыдущемуПослеНачальногоВЛоге__ДолженВернутьВсеЗаписиДоИндексаКоммита(
        int startLsn,
        int commitIndex,
        int logSize)
    {
        var snapshotIndex = startLsn - 1;
        var entries = Enumerable.Range(0, logSize)
                                .Select(i => Entry(1, i.ToString()))
                                .ToArray();
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries: entries,
            snapshotData: ( 1, snapshotIndex, new StubSnapshot([1, 2, 3, 99]) ),
            commitIndex: commitIndex,
            startLsn: startLsn);
        var expected = entries.Take(commitIndex - startLsn + 1)
                              .Select(e => e.Data)
                              .ToArray();

        var actual = facade.ReadCommittedDeltaFromPreviousSnapshot().ToArray();

        Assert.Equal(expected, actual, new ByteArrayEqualityComparer());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void ShouldCreateSnapshot__КогдаЛогПуст__ДолженВернутьFalse(int threshold)
    {
        var (facade, _) = CreateFacade(tailEntries: Array.Empty<LogEntry>(),
            segmentEntries: Array.Empty<IReadOnlyList<LogEntry>>(),
            snapshotOptions: new SnapshotOptions(threshold));

        var actual = facade.ShouldCreateSnapshot();

        Assert.False(actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void
        ShouldCreateSnapshot__КогдаВЛогеЕстьТолькоХвостИИндексКоммитаРавенПоследнейЗаписиВХвосте__ДолженВернутьFalse(
        int threshold)
    {
        var (facade, _) = CreateFacade(
            tailEntries: Enumerable.Range(0, 10).Select(i => Entry(1, i.ToString())).ToArray(),
            segmentEntries: Array.Empty<IReadOnlyList<LogEntry>>(),
            snapshotOptions: new SnapshotOptions(threshold));

        var actual = facade.ShouldCreateSnapshot();

        Assert.False(actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public void
        ShouldCreateSnapshot__КогдаПорогРавен1ИЕстьЗакрытыеСегментыИИндексКоммитаРавенПоследнейЗаписи__ДолженВернутьTrue(
        int segmentsCount)
    {
        var threshold = 1;
        var segmentSize = 128;
        var tailSize = 100;
        var (facade, _) = CreateFacade(
            tailEntries: Enumerable.Range(0, tailSize).Select(i => Entry(1, i.ToString())).ToArray(),
            segmentEntries: Enumerable.Range(0, segmentsCount)
                                      .Select(s => Enumerable.Range(1, segmentSize)
                                                             .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                             .ToArray())
                                      .ToArray(),
            snapshotOptions: new SnapshotOptions(threshold));
        var lastIndex = segmentSize * segmentsCount + tailSize - 1;
        facade.SetCommitTest(lastIndex);

        var actual = facade.ShouldCreateSnapshot();

        Assert.True(actual);
    }

    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 3)]
    [InlineData(2, 4)]
    [InlineData(1, 5)]
    public void ShouldCreateSnapshot__КогдаЗакрытыхСегментовМеньшеПорога__ДолженВернутьFalse(
        int segmentsCount,
        int threshold)
    {
        var segmentSize = 128;
        var tailSize = 100;
        var (facade, _) = CreateFacade(
            tailEntries: Enumerable.Range(0, tailSize).Select(i => Entry(1, i.ToString())).ToArray(),
            segmentEntries: Enumerable.Range(0, segmentsCount)
                                      .Select(s => Enumerable.Range(1, segmentSize)
                                                             .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                             .ToArray())
                                      .ToArray(),
            snapshotOptions: new SnapshotOptions(threshold));
        var lastIndex = segmentSize * segmentsCount - 2; // Коммит на предпоследней записи в последнем сегменте

        facade.SetCommitTest(lastIndex);

        var actual = facade.ShouldCreateSnapshot();

        Assert.False(actual);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 0)]
    [InlineData(2, 1)]
    [InlineData(5, 1)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    public void
        ShouldCreateSnapshot__КогдаСегментовНеМеньшеПорогаНоНеВсеСегментыИзПорогаЗакоммичены__ДолженВернутьFalse(
        int threshold,
        int committedSegmentsCount)
    {
        const int segmentSize = 128;
        const int tailSize = 100;

        // Пусть всего будет столько же сегментов, сколько и порог
        var segmentsCount = threshold;
        var (facade, _) = CreateFacade(tailEntries: Enumerable.Range(0, tailSize)
                                                              .Select(i => Entry(1, i.ToString()))
                                                              .ToArray(),
            segmentEntries: Enumerable.Range(0, segmentsCount)
                                      .Select(s => Enumerable.Range(1, segmentSize)
                                                             .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                             .ToArray())
                                      .ToArray(),
            snapshotOptions: new SnapshotOptions(threshold));
        var lastIndex =
            segmentSize * committedSegmentsCount
          + segmentSize / 2; // Коммит на предпоследней записи в последнем сегменте
        facade.SetCommitTest(lastIndex);

        var actual = facade.ShouldCreateSnapshot();

        Assert.False(actual);
    }

    [Theory]
    [InlineData(1, 1, 1)]
    [InlineData(1, 5, 1)]
    [InlineData(1, 5, 2)]
    [InlineData(1, 5, 5)]
    [InlineData(2, 2, 2)]
    [InlineData(2, 3, 2)]
    [InlineData(5, 10, 5)]
    [InlineData(5, 10, 6)]
    public void ShouldCreateSnapshot__КогдаСегментовНеМеньшеПорогаИВсеСегментыПорогаЗакоммичены__ДолженВернутьTrue(
        int threshold,
        int segmentsCount,
        int committedSegmentsCount)
    {
        const int segmentSize = 128;
        const int tailSize = segmentSize;

        var (facade, _) = CreateFacade(tailEntries: Enumerable.Range(0, tailSize)
                                                              .Select(i => Entry(1, i.ToString()))
                                                              .ToArray(),
            segmentEntries: Enumerable.Range(0, segmentsCount)
                                      .Select(s => Enumerable.Range(1, segmentSize)
                                                             .Select(e => Entry(1, ( s * segmentSize + e ).ToString()))
                                                             .ToArray())
                                      .ToArray(),
            snapshotOptions: new SnapshotOptions(threshold));
        var lastIndex =
            segmentSize * committedSegmentsCount
          + segmentSize / 2; // Коммит на предпоследней записи в последнем сегменте
        facade.SetCommitTest(lastIndex);

        var actual = facade.ShouldCreateSnapshot();

        Assert.True(actual);
    }

    [Fact]
    public void Инициализация__КогдаФайловНеСуществовало__ДолженИнициализироватьБезОшибок()
    {
        var fs = Helpers.CreateFileSystem();

        var ex = Record.Exception(() => FileSystemPersistenceFacade.Initialize(fs.DataDirectory, Logger.None));

        Assert.Null(ex);
    }

    [Fact]
    public void Инициализация__КогдаФайловНеСуществовало__ДолженИнициализироватьПустойЛог()
    {
        var fs = Helpers.CreateFileSystem();

        var facade = FileSystemPersistenceFacade.Initialize(fs.DataDirectory, Logger.None);

        var actual = facade.Log.ReadAllTest();
        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(754)]
    public void Инициализация__КогдаСнапшотаНеБылоИЛогНеПуст__ДолженВыставитьИндексКоммитаTomb(
        int logSize)
    {
        var (old, fs) = CreateFacade(
            tailEntries: Enumerable.Range(0, logSize).Select(_ => Entry(1, "sample")).ToArray());
        old.Dispose();

        var expected = Lsn.Tomb;

        var newFacade = FileSystemPersistenceFacade.Initialize(fs.DataDirectory, Logger.None);
        var actual = newFacade.CommitIndex;

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0, 0, 10)]
    [InlineData(0, 8, 10)]
    [InlineData(10, 9, 10)]
    [InlineData(10, 10, 10)]
    [InlineData(10, 15, 10)]
    [InlineData(10, 19, 10)]
    public void Инициализация__КогдаСнапшотБыл__ДолженВыставитьИндексКоммитаВИндексСнапшота(
        int startLsn,
        int snapshotIndex,
        int logSize)
    {
        var (old, fs) = CreateFacade(startLsn: startLsn,
            snapshotData: ( 1, new Lsn(snapshotIndex), new StubSnapshot([1, 2, 3, 4]) ),
            tailEntries: Enumerable.Range(0, logSize).Select(i => Entry(1, i.ToString())).ToArray());
        old.Dispose();
        var expected = new Lsn(snapshotIndex);

        var newFacade = FileSystemPersistenceFacade.Initialize(fs.DataDirectory, Logger.None);
        var actual = newFacade.CommitIndex;

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(10, 0)]
    [InlineData(14, 13)]
    public void Инициализация__КогдаИндексСнапшотаБольшеПоследнегоИндексаЛога__ДолженОчиститьЛог(
        int snapshotIndex,
        int lastLogIndex)
    {
        var fs = Helpers.CreateFileSystem();
        var log = SegmentedFileLog.InitializeTest(fs.Log, startIndex: lastLogIndex,
            tailEntries: new[] {Entry(1, "asdf")});
        log.Dispose();
        var snapshot = SnapshotFile.Initialize(fs.DataDirectory);
        snapshot.SetupSnapshotTest(1, snapshotIndex, new StubSnapshot([1, 2, 3, 4]));

        var facade = FileSystemPersistenceFacade.Initialize(fs.DataDirectory, Logger.None);

        Assert.Empty(facade.Log.ReadAllTest());
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(10, 0)]
    [InlineData(14, 13)]
    public void
        Инициализация__КогдаИндексСнапшотаБольшеПоследнегоИндексаЛога__ДолженВыставитьИндексКоммитаВИндексСнапшота(
        int snapshotIndex,
        int lastLogIndex)
    {
        var fs = Helpers.CreateFileSystem();
        var log = SegmentedFileLog.InitializeTest(fs.Log, startIndex: lastLogIndex,
            tailEntries: new[] {Entry(1, "asdf")});
        log.Dispose();
        var snapshot = SnapshotFile.Initialize(fs.DataDirectory);
        snapshot.SetupSnapshotTest(1, snapshotIndex, new StubSnapshot([1, 2, 3, 4]));

        var facade = FileSystemPersistenceFacade.Initialize(fs.DataDirectory, Logger.None);

        var actual = facade.CommitIndex;
        Assert.Equal(snapshotIndex, actual);
    }
}