using System.IO.Abstractions;
using System.Text;
using FluentAssertions;
using Serilog.Core;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Persistence.Log;
using TaskFlux.Consensus.Persistence.Metadata;
using TaskFlux.Consensus.Persistence.Snapshot;
using TaskFlux.Consensus.Tests.Infrastructure;
using TaskFlux.Core;

// ReSharper disable UseUtf8StringLiteral
// ReSharper disable StringLiteralTypo

namespace TaskFlux.Consensus.Tests;

[Trait("Category", "Persistence")]
public class FileSystemPersistenceFacadeTests : IDisposable
{
    private static LogEntry EmptyEntry(int term) => new(new Term(term), Array.Empty<byte>());

    private record MockDataFileSystem(
        IFileInfo LogFile,
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

        var logFileCreation = () => FileLog.Initialize(dataDir);
        logFileCreation
           .Should()
           .NotThrow("файл лога должен остаться в корректном состоянии");
        var metadataFileCreation = () => MetadataFile.Initialize(dataDir);
        metadataFileCreation
           .Should()
           .NotThrow("файл метаданных должен остаться в корректном состоянии");
        var snapshotFileCreation = () => SnapshotFile.Initialize(dataDir);
        snapshotFileCreation
           .Should()
           .NotThrow("файл снапшота должен остаться в корректном состоянии");
    }

    private static readonly Term DefaultTerm = Term.Start;

    /// <summary>
    /// Метод для создания фасада с файлами в памяти.
    /// Создает и инициализирует нужную структуру файлов в памяти.
    /// </summary>
    /// <remarks>Создаваемые файлы пустые</remarks>
    private (FileSystemPersistenceFacade Facade, MockDataFileSystem Fs) CreateFacade(
        Term? initialTerm = null,
        NodeId? votedFor = null)
    {
        var fs = Helpers.CreateFileSystem();
        var logStorage = FileLog.Initialize(fs.DataDirectory);

        var metadataStorage = MetadataFile.Initialize(fs.DataDirectory);
        metadataStorage.SetupMetadataTest(initialTerm ?? MetadataFile.DefaultTerm, votedFor);

        var snapshotStorage = SnapshotFile.Initialize(fs.DataDirectory);
        var facade = new FileSystemPersistenceFacade(logStorage, metadataStorage, snapshotStorage, Logger.None);

        return ( facade,
                 new MockDataFileSystem(fs.Log, fs.Metadata, fs.Snapshot, fs.TemporaryDirectory, fs.DataDirectory) );
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

    private static LogEntry Entry(long term, params byte[] data) => new(term, data);

    private static LogEntry Entry(long term, string data = "") =>
        new(new Term(term), Encoding.UTF8.GetBytes(data));

    [Fact]
    public void Append__КогдаЛогНеПустойИНетСнапшота__ДолженВернутьЗаписьСПравильнымИндексом()
    {
        var (facade, _) = CreateFacade(2);
        facade.Log.SetupLogTest(entries: new[]
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
        facade.Log.SetupLogTest(entries: new[]
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

    private static (T[] Left, T[] Right) Split<T>(IReadOnlyList<T> array, int index)
    {
        var leftLength = index + 1;
        var left = new T[leftLength];
        var rightLength = array.Count - index - 1;

        var right = new T[rightLength];
        for (int i = 0; i <= index; i++)
        {
            left[i] = array[i];
        }

        for (int i = index + 1, j = 0; i < array.Count; i++, j++)
        {
            right[j] = array[i];
        }

        return ( left, right );
    }

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

    [Fact]
    public void TryGetFrom__КогдаИндексРавенПервомуВЛоге__ДолженВернутьТребуемыеЗаписи()
    {
        var (facade, _) = CreateFacade(5);

        // 4 записи в буфере с указанными индексами
        var logEntries = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(3), // 2
            RandomDataEntry(4), // 3
        };

        facade.SetupTest(logEntries);
        var index = 2;
        var expected = logEntries[index..];

        var success = facade.TryGetFrom(index, out var actual);

        Assert.True(success);
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 0)]
    [InlineData(10, 0)]
    [InlineData(10, 3)]
    [InlineData(10, 4)]
    [InlineData(10, 9)]
    [InlineData(20, 4)]
    public void TryGetFrom__КогдаИндексВнутриЛога__ДолженВернутьТребуемыеЗаписи(
        int entriesCount,
        long index)
    {
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(RandomDataEntry)
                                .ToArray();
        var (facade, _) = CreateFacade(entriesCount);
        facade.SetupTest(entries);

        var expected = entries[( int ) index..];

        var success = facade.TryGetFrom(index, out var actual);

        Assert.True(success);
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(50)]
    public void TryGetFrom__КогдаИндексСледующийПослеПоследнего__ДолженВернутьПустойМассив(
        int entriesCount)
    {
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(RandomDataEntry)
                                .ToArray();
        var (facade, _) = CreateFacade(entriesCount);
        facade.SetupTest(entries);

        var success = facade.TryGetFrom(entriesCount, out var actual);

        Assert.True(success);
        Assert.Empty(actual);
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

    // TODO: когда будет сегментированный лог сделать тест на то, что снапшот есть, но файл лога не имеет нужных записей,
    // т.е. последний индекс из лога меньше индекса в снапшоте

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

        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.Snapshot.LastApplied
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");
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

        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.Snapshot.LastApplied
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");
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
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    public void TryGetFrom__КогдаУказанныйИндексЯвляетсяСледующимПослеПоследнего__ДолженВернутьПустойМассив(
        int logSize)
    {
        var term = new Term(1);
        var (facade, _) = CreateFacade(term);
        var logEntries = Enumerable.Range(0, logSize)
                                   .Select(_ => RandomDataEntry(term.Value))
                                   .ToArray();

        facade.Log.SetupLogTest(logEntries);

        var success = facade.TryGetFrom(logSize, out var actual);

        Assert.True(success);
        Assert.Empty(actual);
    }

    [Fact]
    public void TryGetFrom__КогдаЛогПустИИндексРавен0__ДолженВернутьПустойМассив()
    {
        var term = new Term(1);
        var (facade, _) = CreateFacade(term);

        var success = facade.TryGetFrom(0, out var actual);

        Assert.True(success);
        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(10, 0)]
    [InlineData(10, 3)]
    [InlineData(10, 9)]
    [InlineData(100, 2)]
    [InlineData(100, 69)]
    public void TryGetFrom__КогдаИндексВнутриЛога__ДолженВернутьЗаписиСУказаннойПозиции(
        int logSize,
        int index)
    {
        var term = new Term(1);
        var (facade, _) = CreateFacade(term);
        var logEntries = Enumerable.Range(0, logSize)
                                   .Select(_ => RandomDataEntry(term))
                                   .ToArray();
        var expected = logEntries.Skip(index).ToArray();
        facade.SetupTest(logEntries);

        var success = facade.TryGetFrom(index, out var actual);

        Assert.True(success);
        Assert.Equal(expected, actual, Comparer);
    }

    private static readonly ISnapshot NullSnapshot = new StubSnapshot(Array.Empty<byte>());

    [Theory]
    [InlineData(0, 0)] // Только 1 запись в снапшоте
    [InlineData(5, 0)] // Нужно с самого начала
    [InlineData(10, 0)]
    [InlineData(5, 5)] // Попадаем на последний индекс в снапшоте
    [InlineData(10, 10)]
    [InlineData(2, 2)]
    [InlineData(5, 3)] // Где-то внутри снапшота
    [InlineData(5, 2)]
    [InlineData(10, 7)]
    [InlineData(10, 9)] // Предпоследняя запись
    [InlineData(5, 4)]
    [InlineData(2, 1)]
    public void TryGetFrom__КогдаПереданныйИндексВходитВГраницыСнапшота__ДолженВернутьFalse(
        int snapshotLastIndex,
        int index)
    {
        var (facade, _) = CreateFacade();
        facade.SetupTest(logEntries: null, snapshotData: ( new Term(1), snapshotLastIndex, NullSnapshot ));

        var success = facade.TryGetFrom(index, out _);
        Assert.False(success);
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
        facade.SetupTest(
            logEntries: Enumerable.Range(1, ( int ) ( snapshotLastEntry.Index + 10 ))
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

    [Fact]
    public void IsUpToDate__КогдаПереданаПоследняяЗаписьИзЛога__ДолженВернутьTrue()
    {
        var (facade, _) = CreateFacade();
        var entries = new LogEntry[]
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
        var entries = new LogEntry[]
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
        var entries = new LogEntry[]
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
        var entries = new LogEntry[]
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
        var entries = new LogEntry[]
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
        var entries = new LogEntry[]
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
}