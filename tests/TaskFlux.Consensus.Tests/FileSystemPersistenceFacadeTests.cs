using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using System.Text;
using FluentAssertions;
using Serilog.Core;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Persistence.Log;
using TaskFlux.Consensus.Persistence.Metadata;
using TaskFlux.Consensus.Persistence.Snapshot;
using TaskFlux.Core;
using Constants = TaskFlux.Consensus.Persistence.Constants;

// ReSharper disable UseUtf8StringLiteral
// ReSharper disable StringLiteralTypo

namespace TaskFlux.Consensus.Tests;

[Trait("Category", "Persistence")]
public class FileSystemPersistenceFacadeTests : IDisposable
{
    private static LogEntry EmptyEntry(int term) => new(new Term(term), Array.Empty<byte>());
    private static readonly LogEntryInfoEqualityComparer InfoComparer = new();

    private record MockDataFileSystem(
        IFileInfo LogFile,
        IFileInfo MetadataFile,
        IFileInfo SnapshotFile,
        IDirectoryInfo TemporaryDirectory);

    private static readonly string BaseDirectory = Path.Combine("var", "lib", "taskflux");
    private static readonly string DataDirectory = Path.Combine(BaseDirectory, "data");

    private MockDataFileSystem? _createdFs;

    public void Dispose()
    {
        if (_createdFs is not var (logFile, metadataFile, snapshotFile, tempDir))
        {
            return;
        }

        var logFileCreation = () => new FileLog(logFile, tempDir);
        logFileCreation
           .Should()
           .NotThrow("файл лога должен остаться в корректном состоянии");
        var metadataFileCreation = () => new MetadataFile(metadataFile.Open(FileMode.Open), Term.Start, null);
        metadataFileCreation
           .Should()
           .NotThrow("файл метаданных должен остаться в корректном состоянии");
        var snapshotFileCreation = () => new SnapshotFile(snapshotFile, tempDir);
        snapshotFileCreation
           .Should()
           .NotThrow("файл снапшота должен остаться в корректном состоянии");
    }

    private MockDataFileSystem CreateFileSystem()
    {
        var fs = new MockFileSystem(new Dictionary<string, MockFileData>() {[DataDirectory] = new MockDirectoryData()});

        var log = fs.FileInfo.New(Path.Combine(DataDirectory, Constants.LogFileName));
        var metadata = fs.FileInfo.New(Path.Combine(DataDirectory, Constants.MetadataFileName));
        var snapshot = fs.FileInfo.New(Path.Combine(DataDirectory, Constants.SnapshotFileName));
        var tempDirectory = fs.DirectoryInfo.New(Path.Combine(DataDirectory, "temporary"));

        log.Create();
        metadata.Create();
        snapshot.Create();
        tempDirectory.Create();

        var mock = new MockDataFileSystem(log, metadata, snapshot, tempDirectory);
        _createdFs = mock;
        return mock;
    }

    private static readonly Term DefaultTerm = Term.Start;

    /// <summary>
    /// Метод для создания фасада с файлами в памяти.
    /// Создает и инициализирует нужную структуру файлов в памяти.
    /// </summary>
    /// <remarks>Создаваемые файлы пустые</remarks>
    private (FileSystemPersistenceFacade Facade, MockDataFileSystem Fs) CreateFacade(
        int? initialTerm = null,
        NodeId? votedFor = null)
    {
        var fs = CreateFileSystem();
        var logStorage = new FileLog(fs.LogFile, fs.TemporaryDirectory);

        var term = initialTerm is null
                       ? DefaultTerm
                       : new Term(initialTerm.Value);
        var metadataStorage = new MetadataFile(fs.MetadataFile.Open(FileMode.OpenOrCreate), term, votedFor);

        var snapshotStorage = new SnapshotFile(fs.SnapshotFile, fs.TemporaryDirectory);
        var facade = new FileSystemPersistenceFacade(logStorage, metadataStorage, snapshotStorage, Logger.None);

        return ( facade, fs );
    }

    private static readonly LogEntryEqualityComparer Comparer = new();

    [Fact]
    public void Append__СПустымЛогом__НеДолженКоммититьЗапись()
    {
        var (facade, _) = CreateFacade();
        var entry = new LogEntry(new Term(2), Array.Empty<byte>());

        facade.Append(entry);

        var actual = facade.Log.GetUncommittedTest().Single();
        Assert.Equal(entry, actual, Comparer);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 0)]
    [InlineData(0, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 0)]
    [InlineData(0, 3)]
    [InlineData(3, 3)]
    [InlineData(5, 5)]
    public void Append__КогдаЕстьСнапшот__ДолженВернутьПравильныйНовыйИндекс(int committedCount, int uncommittedCount)
    {
        var (facade, _) = CreateFacade();
        var lastSnapshotIndex = 10;
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), lastSnapshotIndex,
            new StubSnapshot(Array.Empty<byte>()));
        facade.Log.SetupLogTest(
            committed: Enumerable.Range(0, committedCount).Select(_ => RandomDataEntry(2)).ToArray(),
            uncommitted: Enumerable.Range(0, uncommittedCount).Select(_ => RandomDataEntry(2)).ToArray());

        var expected = new LogEntryInfo(new Term(2), lastSnapshotIndex + committedCount + uncommittedCount + 1);

        var actual = facade.Append(new LogEntry(new Term(2), Array.Empty<byte>()));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Append__КогдаЛогПуст__ДолженДобавитьЗаписьКакНеЗакоммиченную()
    {
        var (facade, _) = CreateFacade();
        var entry = new LogEntry(new Term(2), Array.Empty<byte>());

        facade.Append(entry);

        Assert.Single(facade.Log.ReadAllTest());
    }

    [Fact]
    public void Append__КогдаЛогПуст__ДолженВернутьАктуальнуюИнформациюОЗаписанномЭлементе()
    {
        var entry = new LogEntry(DefaultTerm, new byte[] {123, 4, 56});
        var (facade, _) = CreateFacade();

        // Индексирование начинается с 0
        var expected = new LogEntryInfo(entry.Term, 0);

        var actual = facade.Append(entry);

        Assert.Equal(actual, expected);
    }

    [Fact]
    public void Append__КогдаЕстьНезакоммиченныеЭлементы__ДолженВернутьПравильнуюЗапись()
    {
        var (facade, _) = CreateFacade(2);
        var uncommitted = new List<LogEntry>()
        {
            new(new Term(1), new byte[] {1, 2, 3}), new(new Term(2), new byte[] {4, 5, 6}),
        };
        facade.Log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted);
        var entry = new LogEntry(new Term(3), new byte[] {7, 8, 9});
        var expected = new LogEntryInfo(entry.Term, 2);

        var actual = facade.Append(entry);

        Assert.Equal(expected, actual);
        Assert.Equal(expected, facade.LastEntry);
    }

    private static LogEntry Entry(int term, params byte[] data) => new LogEntry(new Term(term), data);

    private static LogEntry Entry(int term, string data = "") =>
        new(new Term(term), Encoding.UTF8.GetBytes(data));

    [Fact]
    public void Append__КогдаВЛогНеПустойИНетСнапшота__ДолженВернутьЗаписьСПравильнымИндексом()
    {
        var (facade, _) = CreateFacade(2);
        facade.Log.SetupLogTest(committed: new[]
            {
                Entry(1, 99, 76, 33), // 0
                Entry(1, 9),          // 1
                Entry(2, 94, 22, 48)  // 2
            },
            uncommitted: Array.Empty<LogEntry>());
        // Должен вернуть индекс 3
        var entry = Entry(2, "data");
        var expected = new LogEntryInfo(entry.Term, 3);

        var actual = facade.Append(entry);

        Assert.Equal(expected, actual, InfoComparer);
    }

    [Fact]
    public void Append__КогдаВЛогНеПустИСнапшотЕсть__ДолженВернутьЗаписьСПравильнымИндексом()
    {
        var (facade, _) = CreateFacade(2);
        facade.SnapshotStorage
              .SetupSnapshotTest(1, 10, new StubSnapshot(Array.Empty<byte>()));
        facade.Log.SetupLogTest(committed: new[]
            {
                Entry(1, 99, 76, 33), // 11
                Entry(1, 9),          // 12
                Entry(2, 94, 22, 48)  // 13
            },
            uncommitted: Array.Empty<LogEntry>());
        // Должен вернуть индекс 14
        var entry = Entry(2, "data");
        var expected = new LogEntryInfo(entry.Term, 14);

        var actual = facade.Append(entry);

        Assert.Equal(expected, actual, InfoComparer);
    }

    [Fact]
    public void
        Append__КогдаВЛогеЕстьЗакоммиченныеИНеЗакоммиченныеЗаписи__ДолженВернутьПравильныйИндексДобавленнойЗаписи()
    {
        var (facade, _) = CreateFacade(3);
        facade.Log.SetupLogTest(committed: new[]
            {
                Entry(1, "adfasfas"),   // 0
                Entry(2, "aaaa"),       // 1
                Entry(2, "aegqer89987") // 2
            },
            uncommitted: new List<LogEntry>()
            {
                Entry(3, "asdf"), // 3
            });

        var entry = Entry(4, "data");
        var expected = new LogEntryInfo(entry.Term, 4);

        var actual = facade.Append(entry);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void LastEntry__КогдаНетСнапшотаИЛогПуст__ДолженВернутьTomb()
    {
        var (facade, _) = CreateFacade();
        var expected = LogEntryInfo.Tomb;

        var actual = facade.LastEntry;

        Assert.Equal(expected, actual, InfoComparer);
    }

    [Fact]
    public void LastEntry__КогдаНетСнапшотаИЕстьЗаписиВЛоге__ДолженВернутьЗначениеИзЛога()
    {
        var (facade, _) = CreateFacade();
        var committed = new[]
        {
            Entry(1, "hello"), // 0
            Entry(2, "world"), // 1
        };
        facade.Log.SetupLogTest(committed, Array.Empty<LogEntry>());
        var expected = new LogEntryInfo(new Term(2), 1);

        var actual = facade.LastEntry;

        Assert.Equal(expected, actual, InfoComparer);
    }

    [Fact]
    public void LastEntry__КогдаЕстьСнапшотИЛогПуст__ДолженВернутьЗначениеИзСнапшота()
    {
        var (facade, _) = CreateFacade();
        var expected = new LogEntryInfo(new Term(123), 9999);
        facade.SnapshotStorage.SetupSnapshotTest(expected.Term, expected.Index, new StubSnapshot(Array.Empty<byte>()));

        var actual = facade.LastEntry;

        Assert.Equal(expected, actual, InfoComparer);
    }

    [Fact]
    public void LastEntry__КогдаЕстьСнапшотИЛогНеПуст__ДолженВернутьЗначениеСКорректнымГлобальнымИндексом()
    {
        var (facade, _) = CreateFacade();
        facade.SnapshotStorage.SetupSnapshotTest(new Term(10), 123, new StubSnapshot(Array.Empty<byte>()));
        facade.Log.SetupLogTest(committed: new[]
            {
                Entry(11, "first"),  // 124
                Entry(12, "second"), // 125
            },
            uncommitted: new[]
            {
                Entry(13, "asdfasdfasdfasdf") // 126
            });
        var expected = new LogEntryInfo(new Term(13), 126);

        var actual = facade.LastEntry;

        Assert.Equal(expected, actual, InfoComparer);
    }

    private static LogEntry RandomDataEntry(int term)
    {
        var buffer = new byte[Random.Shared.Next(0, 32)];
        Random.Shared.NextBytes(buffer);
        return new LogEntry(new Term(term), buffer);
    }

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
    [InlineData(5, 2)]
    [InlineData(5, 0)]
    [InlineData(5, 4)]
    [InlineData(10, 0)]
    [InlineData(10, 5)]
    [InlineData(10, 9)]
    public void Commit__КогдаСнапшотаНет__ДолженЗакоммититьЗаписиПоУказанномуИндексу(
        int uncommittedCount,
        int commitIndex)
    {
        var (facade, _) = CreateFacade(uncommittedCount);
        var uncommitted = Enumerable.Range(1, uncommittedCount)
                                    .Select(RandomDataEntry)
                                    .ToList();

        var (expectedCommitted, expectedUncommitted) = Split(uncommitted, commitIndex);
        facade.Log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted);

        facade.Commit(commitIndex);

        Assert.Equal(expectedUncommitted, facade.Log.GetUncommittedTest(), Comparer);
        Assert.Equal(expectedCommitted, facade.Log.GetCommittedTest(), Comparer);
        Assert.Equal(commitIndex, facade.CommitIndex);
    }

    [Theory]
    [InlineData(0, 10, 1)]
    [InlineData(0, 10, 5)]
    [InlineData(0, 10, 10)]
    [InlineData(10, 5, 11)]
    [InlineData(10, 5, 15)]
    public void Commit__КогдаСнапшотЕсть__ДолженЗакоммититьЗаписиПоУказанномуИндексу(
        int snapshotIndex,
        int uncommittedCount,
        int commitIndex)
    {
        var (facade, _) = CreateFacade(uncommittedCount);
        var uncommitted = Enumerable.Range(1, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToList();

        var (expectedCommitted, expectedUncommitted) = Split(uncommitted, commitIndex - snapshotIndex - 1);
        facade.Log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted);
        facade.SnapshotStorage.SetupSnapshotTest(new Term(10), snapshotIndex, new StubSnapshot(Array.Empty<byte>()));

        facade.Commit(commitIndex);

        Assert.Equal(expectedUncommitted, facade.Log.GetUncommittedTest(), Comparer);
        Assert.Equal(expectedCommitted, facade.Log.GetCommittedTest(), Comparer);
        Assert.Equal(commitIndex, facade.CommitIndex);
    }

    [Fact]
    public void TryGetFrom__КогдаЗаписиВПамяти__ДолженВернутьТребуемыеЗаписи()
    {
        var (facade, _) = CreateFacade(5);
        // 4 записи в буфере с указанными индексами
        var uncommitted = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(3), // 2
            RandomDataEntry(4), // 3
        };

        facade.Log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted);

        var index = 2;
        var expected = uncommitted[index..];

        var success = facade.TryGetFrom(index, out var actual);

        Assert.True(success);
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(10, 5, 4)] // Часть в файле, часть в памяти
    [InlineData(10, 5, 6)] // Все из памяти
    [InlineData(5, 1, 2)]
    [InlineData(1, 0, 0)]  // Все записи в файле
    [InlineData(2, 0, 0)]  // Одна в файле, одна в памяти, нужны все
    [InlineData(2, 0, 1)]  // Одна в файле, одна в памяти, нужна только из памяти
    [InlineData(5, 1, 4)]  // Только последняя из памяти
    [InlineData(10, 9, 9)] // Все в файле, только 1 запись нужна
    [InlineData(10, 9, 0)] // Все в файле, все записи нужны
    [InlineData(10, 9, 5)] // Все в файле, читаем с середины
    public void TryGetFrom__КогдаЧастьЗаписейВБуфереЧастьВФайле__ДолженВернутьТребуемыеЗаписи(
        int entriesCount,
        int logEndIndex,
        int index)
    {
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(RandomDataEntry)
                                .ToArray();
        var (committed, uncommitted) = Split(entries, logEndIndex);
        var (facade, _) = CreateFacade(entriesCount);

        facade.Log.SetupLogTest(committed, uncommitted);

        // Глобальный и локальный индексы совпадают, если снапшота еще нет
        var expected = entries[index..];

        var success = facade.TryGetFrom(index, out var actual);

        Assert.True(success);
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(10, 10)] // Последняя запись
    [InlineData(2, 2)]
    [InlineData(1, 1)]
    [InlineData(10, 1)] // Первая запись
    [InlineData(5, 1)]
    [InlineData(10, 9)] // Предпоследняя запись
    [InlineData(2, 1)]
    [InlineData(5, 4)]
    [InlineData(10, 5)] // Запись где-то в середине
    [InlineData(5, 3)]
    public void GetPrecedingEntryInfo__КогдаЗаписиВБуфере__ДолженВернутьТребуемуюЗапись(int bufferSize, int entryIndex)
    {
        var (facade, _) = CreateFacade(bufferSize + 1);
        var uncommitted = Enumerable.Range(1, bufferSize)
                                    .Select(RandomDataEntry)
                                    .ToArray();
        facade.Log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted);
        var expected = GetExpected();

        var actual = facade.GetPrecedingEntryInfo(entryIndex);

        Assert.Equal(expected, actual);

        LogEntryInfo GetExpected()
        {
            var e = uncommitted[entryIndex - 1];
            return new LogEntryInfo(e.Term, entryIndex - 1);
        }
    }

    [Theory]
    [InlineData(10, 10)] // Последняя запись
    [InlineData(2, 2)]
    [InlineData(1, 1)]
    [InlineData(10, 1)] // Первая запись
    [InlineData(5, 1)]
    [InlineData(10, 9)] // Предпоследняя запись
    [InlineData(2, 1)]
    [InlineData(5, 4)]
    [InlineData(10, 5)] // Запись где-то в середине
    [InlineData(5, 3)]
    public void GetPrecedingEntryInfo__КогдаВсеЗаписиВХранилище__ДолженВернутьТребуемуюЗапись(
        int logSize,
        int entryIndex)
    {
        var (facade, _) = CreateFacade(logSize + 1);
        var entries = Enumerable.Range(1, logSize)
                                .Select(RandomDataEntry)
                                .ToArray();

        facade.Log.AppendRange(entries);
        var expected = GetExpected();

        var actual = facade.GetPrecedingEntryInfo(entryIndex);

        Assert.Equal(expected, actual);

        LogEntryInfo GetExpected()
        {
            var e = entries[entryIndex - 1];
            return new LogEntryInfo(e.Term, entryIndex - 1);
        }
    }

    [Theory]
    [InlineData(10, 5, 10)]
    [InlineData(10, 5, 5)]
    [InlineData(10, 5, 4)]
    [InlineData(10, 5, 1)]
    [InlineData(2, 0, 2)]
    [InlineData(2, 0, 1)]
    [InlineData(5, 3, 3)]
    [InlineData(5, 3, 2)]
    [InlineData(5, 3, 5)]
    [InlineData(5, 3, 1)]
    public void GetPrecedingEntryInfo__КогдаЗаписиВФайлеИБуфере__ДолженВернутьТребуемуюЗапись(
        int entriesCount,
        int logEndIndex,
        int entryIndex)
    {
        var (facade, _) = CreateFacade(entriesCount + 1);
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(RandomDataEntry)
                                .ToArray();
        var (committed, uncommitted) = Split(entries, logEndIndex);

        facade.Log.SetupLogTest(committed, uncommitted);

        var expected = GetExpected();

        var actual = facade.GetPrecedingEntryInfo(entryIndex);

        Assert.Equal(expected, actual);

        LogEntryInfo GetExpected()
        {
            var e = entries[entryIndex - 1];
            return new LogEntryInfo(e.Term, entryIndex - 1);
        }
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

        Assert.Equal(entries, facade.Log.GetUncommittedTest(), Comparer);
    }

    [Theory]
    [InlineData(1, 5)]
    [InlineData(10, 1)]
    [InlineData(100, 2)]
    [InlineData(1435, 10)]
    public void InsertRange__КогдаСнапшотЕстьИЛогПустИИндексСледующийПослеИндексаСнапшота__ДолженВставитьЗаписиВКонец(
        int snapshotLastIndex,
        int elementsCount)
    {
        var (facade, _) = CreateFacade(elementsCount + 1);
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), snapshotLastIndex,
            new Stubs.StubSnapshot(Array.Empty<byte>()));
        var entries = Enumerable.Range(1, elementsCount)
                                .Select(i => Entry(i, i.ToString()))
                                .ToArray();

        facade.InsertRange(entries, snapshotLastIndex + 1);

        Assert.Equal(entries, facade.Log.GetUncommittedTest(), Comparer);
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
    public void InsertRange__ВКонецЛогаСНеПустымБуфером__ДолженДобавитьЗаписиВБуфер(int bufferSize, int toInsertSize)
    {
        var uncommitted = Enumerable.Range(1, bufferSize)
                                    .Select(RandomDataEntry)
                                    .ToList();

        var toInsert = Enumerable.Range(bufferSize + 1, toInsertSize)
                                 .Select(RandomDataEntry)
                                 .ToList();

        var expected = uncommitted.Concat(toInsert)
                                  .ToList();

        var (facade, _) = CreateFacade(bufferSize + toInsertSize + 1);
        facade.Log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted);

        facade.InsertRange(toInsert, bufferSize);

        var actual = facade.Log.GetUncommittedTest();
        Assert.Equal(expected, actual, Comparer);
        Assert.Empty(facade.Log.GetCommittedTest());
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
    public void InsertRange__СИндексомВнутриНеПустогоБуфера__ДолженВставитьИЗатеретьСтарыеЗаписи(
        int bufferCount,
        int toInsertCount,
        int insertIndex)
    {
        var uncommitted = Enumerable.Range(1, bufferCount)
                                    .Select(EmptyEntry)
                                    .ToList();

        var toInsert = Enumerable.Range(bufferCount + 1, toInsertCount)
                                 .Select(EmptyEntry)
                                 .ToList();

        var expected = uncommitted.Take(insertIndex)
                                  .Concat(toInsert)
                                  .ToList();

        var (facade, _) = CreateFacade(bufferCount + toInsertCount + 1);
        facade.Log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted);

        facade.InsertRange(toInsert, insertIndex);

        var actual = facade.Log.GetUncommittedTest();

        Assert.Equal(expected, actual, Comparer);
        Assert.Empty(facade.Log.GetCommittedTest());
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
        var data = new byte[] {1, 2, 3};
        facade.Log.SetupLogTest(logEntries, Array.Empty<LogEntry>());

        facade.SaveSnapshot(new StubSnapshot(data), new LogEntryInfo(6, 3));

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        Assert.True(fs.SnapshotFile.Exists);
        Assert.Equal(expectedLastEntry.Index, actualIndex);
        Assert.Equal(expectedLastEntry.Term, actualTerm);
        Assert.Equal(data, actualData);
        Assert.Equal(expectedLastEntry, facade.SnapshotStorage.LastApplied);
    }

    [Fact]
    public void SaveSnapshot__КогдаСнапшотСуществовалИИндексКоммитаРавенВсемуЛогу__ДолженКорректно()
    {
        var oldSnapshotEntry = new LogEntryInfo(5, 10);
        var oldSnapshot = new StubSnapshot(RandomBytes(100));
        var committed = new[]
        {
            RandomDataEntry(1), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
            RandomDataEntry(4), // 14
            RandomDataEntry(5), // 15
        };

        var data = new byte[128];
        Random.Shared.NextBytes(data);

        var (facade, fs) = CreateFacade();
        facade.Log.SetupLogTest(committed, Array.Empty<LogEntry>());
        facade.SnapshotStorage.SetupSnapshotTest(oldSnapshotEntry.Term, oldSnapshotEntry.Index, oldSnapshot);

        var newSnapshotEntry = new LogEntryInfo(new Term(3), 15);
        facade.SaveSnapshot(new StubSnapshot(data), newSnapshotEntry);

        // var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        // Assert.Equal(snapshotEntry.Index, actualIndex);
        // Assert.Equal(snapshotEntry.Term, actualTerm);
        // Assert.Equal(data, actualData);
        // Assert.Equal(snapshotEntry, facade.SnapshotStorage.LastApplied);

        // TODO: да блять ошибка здесь нахуй!!!!!!!!!
        var newEntry = new LogEntry(new Term(3), new byte[] {123, 22, 22});
        var newEntryIndex = 16;
        facade.InsertRange(new[] {newEntry, new LogEntry(4, "asdfasdf"u8.ToArray())}, newEntryIndex);
        facade.Commit(newEntryIndex + 1);

        // var x = new FileLog(fs.LogFile, fs.TemporaryDirectory);
        var x = facade.Log;
        var asdf = x.GetCommittedTest();
        var asffggg = x.GetUncommittedTest();
        var commitIndex = x.CommitIndex;
        var value = x.ReadAllTest();
        var fggg = x.ReadCommitIndexTest();
        Console.WriteLine($"");
    }

    [Fact]
    public void SaveSnapshot__КогдаФайлСнапшотаСуществовалПустой__ДолженПерезаписатьСтарыйФайл()
    {
        var committed = new[]
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
        facade.Log.SetupLogTest(committed, Array.Empty<LogEntry>());

        var snapshotEntry = new LogEntryInfo(new Term(3), 4);
        facade.SaveSnapshot(new StubSnapshot(data), snapshotEntry);

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        Assert.Equal(snapshotEntry.Index, actualIndex);
        Assert.Equal(snapshotEntry.Term, actualTerm);
        Assert.Equal(data, actualData);
        Assert.Equal(snapshotEntry, facade.SnapshotStorage.LastApplied);
    }

    [Fact]
    public void CreateSnapshot__КогдаВЛогеБылиЗаписи__ДолженУдалитьВключенныеВСнапшотКоманды()
    {
        /*
         * Снапшота нет.
         * Индексы в логе начинаются с 0.
         * Включенный в снапшот индекс - 50.
         * В логе - 100 записей.
         */
        var (facade, _) = CreateFacade();
        var lastSnapshotEntry = new LogEntryInfo(new Term(7), 50);
        var logEntries = Enumerable.Range(1, 100)
                                   .Select(i => Entry(i, i.ToString()))
                                   .ToArray();
        var snapshotData = RandomBytes(100);
        facade.Log.SetupLogTest(logEntries, Array.Empty<LogEntry>());
        var expectedLogEntries = logEntries.Skip(lastSnapshotEntry.Index + 1)
                                           .ToArray();
        var writer = facade.CreateSnapshot(lastSnapshotEntry);

        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var actualCommittedLog = facade.Log.GetCommittedTest();
        Assert.Equal(expectedLogEntries, actualCommittedLog, Comparer);
    }

    [Fact]
    public void
        CreateSnapshot__КогдаСнапшотБылИИндексНовогоСнапшотаРавенИндексуПоследнейЗакоммиченнойЗаписи__ДолженКорректноОбновитьИндексыКоммита()
    {
        var (facade, _) = CreateFacade();
        var oldSnapshotEntry = new LogEntryInfo(10, 120);
        var newSnapshotEntry = new LogEntryInfo(12, 140);
        var committed = Enumerable.Range(1, 40)
                                  .Select(i => Entry(12, i.ToString()))
                                  .ToArray();
        var newSnapshotData = RandomBytes(100);
        var oldSnapshotData = RandomBytes(123);
        facade.Log.SetupLogTest(committed, Array.Empty<LogEntry>());
        facade.SnapshotStorage.SetupSnapshotTest(oldSnapshotEntry.Term, oldSnapshotEntry.Index,
            new StubSnapshot(oldSnapshotData));
        var expectedLogEntries = committed[20..];
        var expectedLogCommitIndex = 19;

        var writer = facade.CreateSnapshot(newSnapshotEntry);
        var stubSnapshot = new StubSnapshot(newSnapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var actualLogEntries = facade.Log.ReadAllTest();
        Assert.Equal(expectedLogEntries, actualLogEntries, Comparer);
        var actualLogCommitIndex = facade.Log.CommitIndex;
        Assert.Equal(expectedLogCommitIndex, actualLogCommitIndex);
    }

    [Fact]
    public void CreateSnapshot__КогдаСнапшотаНеБылоИЛогПуст__ДолженСоздатьНовыйСнапшот()
    {
        var (facade, _) = CreateFacade();
        var lastSnapshotEntry = new LogEntryInfo(new Term(7), 10);
        var snapshotData = RandomBytes(100);

        var writer = facade.CreateSnapshot(lastSnapshotEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var (actualLastIndex, actualLastTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        Assert.Equal(snapshotData, actualData);
        Assert.Equal(lastSnapshotEntry.Term, actualLastTerm);
        Assert.Equal(lastSnapshotEntry.Index, actualLastIndex);
    }

    [Fact]
    public void CreateSnapshot__КогдаФайлСнапшотаСуществовалНеПустой__ДолженСохранитьДанныеСнапшота()
    {
        var (facade, _) = CreateFacade();
        facade.SnapshotStorage.SetupSnapshotTest(new Term(4), 3, new StubSnapshot(new byte[] {1, 2, 3}));
        var lastLogEntry = new LogEntryInfo(new Term(7), 10);
        var snapshotData = RandomBytes(123);

        var writer = facade.CreateSnapshot(lastLogEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var (actualLastIndex, actualLastTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();

        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.SnapshotStorage.LastApplied
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");
    }

    [Theory]
    [InlineData(1, 2, 2)]
    [InlineData(10, 8, 8)]
    [InlineData(10, 8, 7)]
    [InlineData(643, 543, 123)]
    public void
        CreateSnapshot__КогдаСнапшотаНетИИндексВСнапшотеМеньшеКоличестваЭлементовВЛоге__ДолженУдалитьЗаписиИзЛогаДоУказанногоИндекса(
        int lastSnapshotIndex,
        int committedCount,
        int uncommittedCount)
    {
        var (facade, _) = CreateFacade();

        var snapshotData = RandomBytes(123);

        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(1 + committedCount, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        facade.Log.SetupLogTest(committed, uncommitted);
        var lastSnapshotEntry = new LogEntryInfo(new Term(5), lastSnapshotIndex);
        var expected = committed.Concat(uncommitted).Skip(lastSnapshotIndex + 1).ToArray();

        var writer = facade.CreateSnapshot(lastSnapshotEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var actual = facade.Log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void CreateSnapshot__КогдаФайлаСнапшотаНеСуществовало__ДолженСохранитьДанныеСнапшота()
    {
        // Должен удалить предшествующие записи в логе
        var (facade, _) = CreateFacade();

        var lastLogEntry = new LogEntryInfo(new Term(1), 10);
        var snapshotData = RandomBytes(123);

        var writer = facade.CreateSnapshot(lastLogEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        var (actualLastIndex, actualLastTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();

        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.SnapshotStorage.LastApplied
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");
    }

    [Fact]
    public void InstallSnapshot__КогдаВФайлеЛогаБылиПересекающиесяКоманды__ДолженОчиститьЛогДоУказанныхВСнапшотеКоманд()
    {
        // Должен удалить предшествующие записи в логе
        var (facade, _) = CreateFacade();

        var snapshotData = RandomBytes(123);

        // Снапшота нет, поэтому индексирование с 0
        var existingLog = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(3), // 2
            RandomDataEntry(3), // 3
            RandomDataEntry(3), // 4
        };
        facade.Log.SetupLogTest(existingLog, Array.Empty<LogEntry>());
        // В снапшоте - все команды до 4-ой (индекс 3)
        var lastLogEntry = new LogEntryInfo(new Term(3), 3);
        var expectedLog = existingLog[4..];

        var writer = facade.CreateSnapshot(lastLogEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        // Проверка корректности общей работы
        var (actualLastIndex, actualLastTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.SnapshotStorage.LastApplied
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");

        // Проверка корректности обновления лога
        var actualLog = facade.Log.GetCommittedTest();
        actualLog
           .Should()
           .Equal(expectedLog, LogEntryComparisonFunc, "файл лога должен очиститься, до указанной команды");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(8)]
    [InlineData(9)]
    public void
        CreateSnapshot__КогдаСнапшотаНетИИндексНовогоСнапшотаБольшеПервогоИндексаВЛоге__ДолженОчиститьЛогДоУказанногоИндекса(
        int lastSnapshotIndex)
    {
        var (facade, _) = CreateFacade();

        var snapshotData = RandomBytes(123);

        var committed = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(3), // 2
            RandomDataEntry(3), // 3
            RandomDataEntry(3), // 4
        };

        var uncommitted = new[]
        {
            RandomDataEntry(3), // 5
            RandomDataEntry(4), // 6
            RandomDataEntry(5), // 7
            RandomDataEntry(5), // 8
            RandomDataEntry(5), // 9
        };
        facade.Log.SetupLogTest(committed, uncommitted);

        var lastSnapshotEntry = new LogEntryInfo(new Term(5), lastSnapshotIndex);
        var expectedLog = committed.Concat(uncommitted)
                                   .Skip(lastSnapshotIndex + 1)
                                   .ToArray();

        var writer = facade.CreateSnapshot(lastSnapshotEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        // Проверка корректности обновления лога
        var actualLog = facade.Log.ReadAllTest();
        Assert.Equal(expectedLog, actualLog, Comparer);
    }

    [Theory]
    [InlineData(10)]
    [InlineData(11)]
    public void CreateSnapshot__КогдаИндексВСнапшотеБольшеПоследнегоИндексаВЛоге__ДолженОчиститьЛог(
        int lastSnapshotIndex)
    {
        // Должен удалить предшествующие записи в логе
        var (facade, _) = CreateFacade();

        var snapshotData = RandomBytes(123);

        var committed = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(3), // 2
            RandomDataEntry(3), // 3
            RandomDataEntry(3), // 4
        };

        var uncommitted = new[]
        {
            RandomDataEntry(3), // 5
            RandomDataEntry(4), // 6
            RandomDataEntry(5), // 7
            RandomDataEntry(5), // 8
            RandomDataEntry(5), // 9
        };
        facade.Log.SetupLogTest(committed, uncommitted);

        var lastSnapshotEntry = new LogEntryInfo(new Term(5), lastSnapshotIndex);

        var writer = facade.CreateSnapshot(lastSnapshotEntry);
        var stubSnapshot = new StubSnapshot(snapshotData);
        foreach (var chunk in stubSnapshot.GetAllChunks())
        {
            writer.InstallChunk(chunk.Span, CancellationToken.None);
        }

        writer.Commit();

        // Проверка корректности обновления лога
        var actualLog = facade.Log.ReadAllTest();
        Assert.Empty(actualLog);
    }

    private static byte[] RandomBytes(int size)
    {
        var bytes = new byte[size];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }

    [Fact]
    public void
        SaveSnapshot__КогдаФайлСнапшотаСуществовалНеПустойИИндексПримененнойКомандыПоследний__ДолженПерезаписатьСтарыйФайл()
    {
        var newSnapshotData = new byte[128];
        Random.Shared.NextBytes(newSnapshotData);

        var (facade, _) = CreateFacade();
        // Cуществует старый файл снапшота 
        var oldData = new byte[164];
        Random.Shared.NextBytes(oldData);
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), 3, new StubSnapshot(oldData));

        // У нас в логе 4 команды, причем применены все
        var lastTerm = new Term(5);
        var exisingLogData = new[]
        {
            RandomDataEntry(3),              // 4
            RandomDataEntry(3),              // 5
            RandomDataEntry(4),              // 6
            RandomDataEntry(lastTerm.Value), // 7
        };
        facade.Log.SetupLogTest(exisingLogData, Array.Empty<LogEntry>());
        // Все команды применены из лога

        var snapshotLastEntry = new LogEntryInfo(4, 7);
        facade.SaveSnapshot(new StubSnapshot(newSnapshotData), snapshotLastEntry);

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        Assert.Equal(snapshotLastEntry.Index, actualIndex);
        Assert.Equal(snapshotLastEntry.Term, actualTerm);
        Assert.Equal(newSnapshotData, actualData);
        Assert.Equal(snapshotLastEntry, facade.SnapshotStorage.LastApplied);
    }


    [Fact]
    public void
        SaveSnapshot__КогдаФайлСнапшотаСуществовалНеПустойИИндексПримененнойКомандыВСерединеЛога__ДолженПерезаписатьСтарыйФайл()
    {
        var newSnapshotData = new byte[128];
        Random.Shared.NextBytes(newSnapshotData);

        var (facade, _) = CreateFacade();
        var currentTerm = new Term(5);
        // Cуществует старый файл снапшота 
        var oldData = new byte[164];
        Random.Shared.NextBytes(oldData);
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), 3, new StubSnapshot(oldData));
        facade.UpdateState(currentTerm, null);

        // У нас в логе 4 команды, причем применены все
        var exisingLogData = new[]
        {
            RandomDataEntry(3), // 4
            RandomDataEntry(3), // 5
            RandomDataEntry(4), // 6
            RandomDataEntry(5), // 7
        };
        facade.Log.SetupLogTest(exisingLogData, Array.Empty<LogEntry>());
        // Все команды применены из лога

        facade.SaveSnapshot(new StubSnapshot(newSnapshotData), new LogEntryInfo(3, 5));

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        var expectedIndex = 5;
        var expectedTerm = new Term(3);
        Assert.Equal(expectedIndex, actualIndex);
        Assert.Equal(expectedTerm, actualTerm);
        Assert.Equal(newSnapshotData, actualData);
        Assert.Equal(new LogEntryInfo(expectedTerm, expectedIndex), facade.SnapshotStorage.LastApplied);
    }

    [Theory]
    [InlineData(1, 1, 1)]
    [InlineData(1, 1, 2)]
    [InlineData(5, 5, 1)]
    [InlineData(5, 5, 5)]
    [InlineData(10, 10, 1)]
    [InlineData(10, 10, 5)]
    [InlineData(10, 10, 9)]
    [InlineData(10, 10, 10)]
    [InlineData(10, 10, 11)]
    [InlineData(10, 10, 15)]
    [InlineData(10, 10, 21)]
    public void TryGetFrom__КогдаИндексСнапшота0__ДолженВернутьПравильныеЗаписи(
        int fileCommandsCount,
        int bufferCommandsCount,
        int globalIndex)
    {
        TryGetFromBaseTest(0, fileCommandsCount, bufferCommandsCount, globalIndex);
    }


    [Theory]
    [InlineData(1, 1, 2)]
    [InlineData(1, 1, 3)]
    [InlineData(1, 1, 4)]
    [InlineData(5, 5, 2)]
    [InlineData(5, 5, 5)]
    [InlineData(10, 10, 2)]
    [InlineData(10, 10, 5)]
    [InlineData(10, 10, 9)]
    [InlineData(10, 10, 10)]
    [InlineData(10, 10, 11)]
    [InlineData(10, 10, 15)]
    [InlineData(10, 10, 21)]
    [InlineData(10, 10, 22)]
    public void TryGetFrom__КогдаИндексСнапшота1__ДолженВернутьПравильныеЗаписи(
        int fileCommandsCount,
        int bufferCommandsCount,
        int globalIndex)
    {
        TryGetFromBaseTest(1, fileCommandsCount, bufferCommandsCount, globalIndex);
    }

    [Theory]
    [InlineData(10, 10, 10)]
    [InlineData(0, 0, 0)]
    [InlineData(1, 0, 0)]
    [InlineData(2, 0, 0)]
    [InlineData(100, 0, 0)]
    [InlineData(50, 1, 1)]
    [InlineData(50, 1, 0)]
    [InlineData(50, 0, 1)]
    [InlineData(50, 2, 1)]
    [InlineData(50, 1, 2)]
    public void TryGetFrom__КогдаУказанныйИндексЯвляетсяСледующимПослеПоследнего__ДолженУспешноВернутьПустойМассив(
        int snapshotLastIndex,
        int fileCommandsCount,
        int bufferCommandsCount)
    {
        var term = new Term(1);
        var (facade, _) = CreateFacade(term.Value);
        var fileEntries = Enumerable.Range(0, fileCommandsCount)
                                    .Select(_ => RandomDataEntry(term.Value))
                                    .ToArray();

        var bufferEntries = Enumerable.Range(0, bufferCommandsCount)
                                      .Select(_ => RandomDataEntry(term.Value))
                                      .ToArray();

        facade.SnapshotStorage.SetupSnapshotTest(term,
            snapshotLastIndex,
            new StubSnapshot(Array.Empty<byte>()));

        facade.Log.SetupLogTest(fileEntries, bufferEntries);

        var success = facade.TryGetFrom(snapshotLastIndex + fileCommandsCount + bufferCommandsCount + 1,
            out var actual);

        Assert.True(success);
        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(100, 1, 1, 101)]
    [InlineData(100, 1, 1, 102)]
    [InlineData(123, 5, 5, 124)]
    [InlineData(999, 5, 5, 1004)]
    [InlineData(120000, 10, 10, 120001)]
    [InlineData(456362312, 10, 10, 456362332)]
    [InlineData(3253, 10, 10, 3260)]
    [InlineData(987654, 10, 10, 987664)]
    [InlineData(1423673, 110, 10, 1423784)]
    [InlineData(543546, 10, 10, 543557)]
    [InlineData(2147483000, 10, 10, 2147483001)]
    public void TryGetFrom__КогдаИндексСнапшотаБольшой__ДолженВернутьПравильныеЗаписи(
        int snapshotLastIndex,
        int fileCommandsCount,
        int bufferCommandsCount,
        int globalIndex)
    {
        TryGetFromBaseTest(snapshotLastIndex, fileCommandsCount, bufferCommandsCount, globalIndex);
    }

    private void TryGetFromBaseTest(
        int snapshotLastIndex,
        int fileCommandsCount,
        int bufferCommandsCount,
        int globalIndex)
    {
        var term = new Term(1);
        var (facade, _) = CreateFacade(term.Value);
        var fileEntries = Enumerable.Range(0, fileCommandsCount)
                                    .Select(_ => RandomDataEntry(term.Value))
                                    .ToArray();

        var bufferEntries = Enumerable.Range(0, bufferCommandsCount)
                                      .Select(_ => RandomDataEntry(term.Value))
                                      .ToArray();

        facade.SnapshotStorage.SetupSnapshotTest(term,
            snapshotLastIndex,
            new StubSnapshot(Array.Empty<byte>()));

        facade.Log.SetupLogTest(fileEntries, bufferEntries);

        var expected = fileEntries
                      .Concat(bufferEntries)
                      .Skip(globalIndex - snapshotLastIndex - 1)
                      .ToArray();

        var success = facade.TryGetFrom(globalIndex, out var actual);

        Assert.True(success);
        Assert.Equal(expected, actual, Comparer);
    }

    private static readonly ISnapshot NullSnapshot = new StubSnapshot(Array.Empty<byte>());

    private static readonly Func<LogEntry, LogEntry, bool> LogEntryComparisonFunc = (entry, logEntry) =>
        Comparer.Equals(entry, logEntry);

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
        facade.SnapshotStorage.SetupSnapshotTest(new Term(1), snapshotLastIndex, NullSnapshot);

        var success = facade.TryGetFrom(index, out _);
        Assert.False(success);
    }

    [Fact]
    public void
        GetPrecedingEntryInfo__КогдаСуществуетСнапшот_ИндексРавенПервомуВЛоге__ДолженВернутьВхождениеИзСнапшота()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.SetupSnapshotTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);

        var actual = facade.GetPrecedingEntryInfo(snapshotLastEntry.Index + 1);

        Assert.Equal(snapshotLastEntry, actual);
    }

    [Fact]
    public void
        GetPrecedingEntryInfo__КогдаСуществуетСнапшот_ИндексНаходитсяВПределахЛога__ДолженВернутьВхождениеИзЛога()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.SetupSnapshotTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var committed = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        facade.Log.SetupLogTest(committed, uncommitted: Array.Empty<LogEntry>());
        var expected = new LogEntryInfo(new Term(2), 12);

        var actual = facade.GetPrecedingEntryInfo(13);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void GetPrecedingEntryInfo__КогдаСуществуетСнапшот_ИндексПервыйИзБуфера__ДолженВернутьВхождениеИзЛога()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.SetupSnapshotTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var committed = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        facade.Log.SetupLogTest(committed, uncommitted: Array.Empty<LogEntry>());
        var expected = new LogEntryInfo(new Term(3), 13);

        var actual = facade.GetPrecedingEntryInfo(14);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void GetPrecedingEntryInfo__КогдаСуществуетСнапшот_ИндексВПределахБуфера__ДолженВернутьВхождениеИзБуфера()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.SetupSnapshotTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var committed = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        var uncommitted = new[]
        {
            RandomDataEntry(3), // 14
            RandomDataEntry(3), // 15
            RandomDataEntry(4), // 16
        };

        facade.Log.SetupLogTest(committed, uncommitted);
        var expected = new LogEntryInfo(new Term(3), 15);

        var actual = facade.GetPrecedingEntryInfo(16);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void
        GetPrecedingEntryInfo__КогдаСуществуетСнапшот_ИндексПослеПоследнегоВБуфере__ДолженВернутьВхождениеИзБуфера()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.SetupSnapshotTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var committed = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        var uncommitted = new[]
        {
            RandomDataEntry(3), // 14
            RandomDataEntry(3), // 15
            RandomDataEntry(4), // 16
        };

        facade.Log.SetupLogTest(committed, uncommitted);
        var expected = new LogEntryInfo(new Term(4), 16);

        var actual = facade.GetPrecedingEntryInfo(17);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void GetPrecedingEntryInfo__КогдаИндексРавенСледующемуИзСнапшота__ДолженВернутьДанныеЗаписьИзСнапшота()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.SetupSnapshotTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var uncommitted = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        facade.Log.SetupLogTest(committed: Array.Empty<LogEntry>(),
            uncommitted);
        var expected = new LogEntryInfo(new Term(2), 10);

        var actual = facade.GetPrecedingEntryInfo(11);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    public void InsertRange__КогдаЕстьСнапшотИИндексПоследнийВЛоге__ДолженДобавитьЗаписиВКонец(int uncommittedCount)
    {
        var (facade, _) = CreateFacade();
        var snapshotLastIndex = 11;
        facade.SnapshotStorage.SetupSnapshotTest(new Term(3), snapshotLastIndex,
            new StubSnapshot(Array.Empty<byte>()));
        var uncommitted = Enumerable.Range(0, uncommittedCount)
                                    .Select(_ => RandomDataEntry(3))
                                    .ToArray();
        facade.Log.SetupLogTest(Array.Empty<LogEntry>(), uncommitted);
        var toInsert = new[] {RandomDataEntry(3), RandomDataEntry(3),};
        var expected = uncommitted.Concat(toInsert).ToArray();

        facade.InsertRange(toInsert, snapshotLastIndex + uncommittedCount + 1);

        var actual = facade.Log.GetUncommittedTest();

        actual.Should()
              .Equal(expected, LogEntryComparisonFunc, "записи должны сконкатенироваться");
    }

    [Fact]
    public void InsertRange__КогдаЕстьСнапшот__ДолженПерезаписатьНезакоммиченныеЗаписи()
    {
        var (facade, _) = CreateFacade();
        facade.SnapshotStorage.SetupSnapshotTest(new Term(3), 11,
            new StubSnapshot(Array.Empty<byte>()));
        var uncommittedEntries = new[]
        {
            RandomDataEntry(3), // 12
            RandomDataEntry(4), // 13
            RandomDataEntry(4), // 14
            RandomDataEntry(4), // 15
        };
        facade.Log.SetupLogTest(Array.Empty<LogEntry>(), uncommittedEntries);
        var toInsert = new[] {RandomDataEntry(3), RandomDataEntry(3),};
        var expected = uncommittedEntries.Take(1).Concat(toInsert).ToArray();

        facade.InsertRange(toInsert, 13);

        var actual = facade.Log.GetUncommittedTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void InsertRange__КогдаЕстьСнапшотИИндексСледующийПослеПоследнего__ДолженДобавитьЗаписиВКонецЛога()
    {
        var (facade, _) = CreateFacade();
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), 630,
            new StubSnapshot(Array.Empty<byte>()));
        var committed = new[]
        {
            RandomDataEntry(2), // 631
            RandomDataEntry(2), // 632
        };
        facade.Log.SetupLogTest(committed, Array.Empty<LogEntry>());
        var toInsert = new[] {RandomDataEntry(2),};
        var expected = toInsert;

        facade.InsertRange(toInsert, 633);

        var actual = facade.Log.GetUncommittedTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void SaveSnapshot__КогдаСнапшотУжеСуществовал__ДолженПерезаписатьФайл()
    {
        var (facade, _) = CreateFacade();
        var oldSnapshot = new StubSnapshot(RandomBytes(100));
        var newSnapshot = new StubSnapshot(RandomBytes(90));
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), 10, oldSnapshot);
        facade.Log.SetupLogTest(committed: new[]
            {
                RandomDataEntry(2), // 11
                RandomDataEntry(3), // 12
            },
            uncommitted: Array.Empty<LogEntry>());

        facade.SaveSnapshot(newSnapshot, new LogEntryInfo(3, 12));

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        actualIndex
           .Should()
           .Be(12, "последний примененный индекс - 12");
        actualTerm
           .Should()
           .Be(new Term(3), "терм последней примененной команды - 3");
        actualData
           .Should()
           .Equal(newSnapshot.Data, "данные должны быть перезаписаны");
    }

    [Fact]
    public void SaveSnapshot__КогдаИндексПримененнойКомандыРавенПоследнемуЗакоммиченному__ДолженОчиститьЛог()
    {
        var (facade, _) = CreateFacade();
        var oldSnapshot = new StubSnapshot(RandomBytes(100));
        var newSnapshot = new StubSnapshot(RandomBytes(90));
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), 10, oldSnapshot);
        facade.Log.SetupLogTest(new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(3), // 12
        }, uncommitted: Array.Empty<LogEntry>());

        facade.SaveSnapshot(newSnapshot, new LogEntryInfo(3, 12));

        var actualLogFile = facade.Log.GetCommittedTest();
        actualLogFile.Should()
                     .BeEmpty("последний примененный индекс равен последнему индексу в логе");
    }

    [Fact]
    public void
        SaveSnapshot__КогдаИндексПримененнойКомандыМеньшеПоследнейЗакоммиченной__ДолженОчиститьЛогДоУказанногоИндекса()
    {
        var (facade, _) = CreateFacade();
        var oldSnapshot = new StubSnapshot(RandomBytes(100));
        var newSnapshot = new StubSnapshot(RandomBytes(90));
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), 10, oldSnapshot);
        var lastLogEntry = RandomDataEntry(3);
        var expected = new[] {lastLogEntry};
        facade.Log.SetupLogTest(new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
            lastLogEntry,       // 14
        }, uncommitted: Array.Empty<LogEntry>());

        facade.SaveSnapshot(newSnapshot, new LogEntryInfo(3, 13));

        var actualLogFile = facade.Log.GetCommittedTest();
        actualLogFile.Should()
                     .Equal(expected, EqualityComparison,
                          "последняя примененная запись имеет предпоследний индекс в логе");
    }

    [Theory]
    [InlineData(5, 6, -1)]
    [InlineData(5, 7, 0)]
    [InlineData(10, 20, 8)]
    public void
        SaveSnapshot__КогдаСнапшотаНеБылоИЕстьЗакоммиченныеКомандыИИндексСнапшотаМеньшеЗакоммиченногоИндекса__ДолженКорректноОбновитьИндексыКоммита(
        int snapshotIndex,
        int committedCount,
        int expectedLocalCommitIndex)
    {
        var (facade, _) = CreateFacade();
        var snapshotData = new StubSnapshot(RandomBytes(90));
        facade.Log.SetupLogTest(committed: Enumerable.Range(1, committedCount)
                                                     .Select(i => Entry(2, i.ToString()))
                                                     .ToArray(),
            uncommitted: Array.Empty<LogEntry>());
        var initialCommitIndex = committedCount - 1;

        facade.SaveSnapshot(snapshotData, new LogEntryInfo(2, snapshotIndex));

        // Изначальный индекс коммита поменяться не должен
        Assert.Equal(initialCommitIndex, facade.CommitIndex);

        // Измениться должен индекс коммита лога
        Assert.Equal(expectedLocalCommitIndex, facade.Log.ReadCommitIndexTest());
        Assert.Equal(expectedLocalCommitIndex, facade.Log.CommitIndex);
    }

    [Theory]
    [InlineData(10, 18, 8, -1)]
    [InlineData(10, 17, 8, 0)]
    [InlineData(10, 16, 8, 1)]
    [InlineData(10, 15, 8, 2)]
    [InlineData(10, 14, 8, 3)]
    [InlineData(10, 14, 9, 4)]
    public void
        SaveSnapshot__КогдаСнапшотБылИЕстьЗакоммиченныеЗаписиИИндексСнапшотаМеньшеЗакоммиченногоИндекса__ДолженКорректноОбновитьИндексыКоммита(
        int oldSnapshotIndex,
        int newSnapshotIndex,
        int committedCount,
        int expectedLocalCommitIndex)
    {
        var (facade, _) = CreateFacade();
        var snapshotData = new StubSnapshot(RandomBytes(90));
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), oldSnapshotIndex, new StubSnapshot(Array.Empty<byte>()));
        facade.Log.SetupLogTest(committed: Enumerable.Range(1, committedCount)
                                                     .Select(i => Entry(2, i.ToString()))
                                                     .ToArray(),
            uncommitted: Array.Empty<LogEntry>());
        var initialCommitIndex = facade.CommitIndex;

        facade.SaveSnapshot(snapshotData, new LogEntryInfo(2, newSnapshotIndex));

        // Изначальный индекс коммита поменяться не должен
        Assert.Equal(initialCommitIndex, facade.CommitIndex);

        // Измениться должен индекс коммита лога
        Assert.Equal(expectedLocalCommitIndex, facade.Log.ReadCommitIndexTest());
        Assert.Equal(expectedLocalCommitIndex, facade.Log.CommitIndex);
    }

    [Theory]
    [InlineData(10, 18, 8)]
    [InlineData(10, 19, 8)]
    [InlineData(10, 20, 8)]
    public void
        SaveSnapshot__КогдаСнапшотаБылИЕстьЗакоммиченныеКомандыИИндексСнапшотаБольшеЗакоммиченногоИндекса__ДолженКорректноОбновитьИндексКоммита(
        int oldSnapshotIndex,
        int newSnapshotIndex,
        int committedCount)
    {
        var (facade, _) = CreateFacade();
        var snapshotData = new StubSnapshot(RandomBytes(90));
        facade.SnapshotStorage.SetupSnapshotTest(new Term(2), oldSnapshotIndex, new StubSnapshot(Array.Empty<byte>()));
        facade.Log.SetupLogTest(committed: Enumerable.Range(1, committedCount)
                                                     .Select(i => Entry(2, i.ToString()))
                                                     .ToArray(),
            uncommitted: Array.Empty<LogEntry>());
        var expectedLogCommitIndex = -1; // Все закоммиченные записи были затерты
        var expectedCommitIndex = newSnapshotIndex;

        facade.SaveSnapshot(snapshotData, new LogEntryInfo(2, newSnapshotIndex));

        // Изначальный индекс коммита поменяться не должен
        Assert.Equal(expectedCommitIndex, facade.CommitIndex);

        // Измениться должен индекс коммита лога
        Assert.Equal(expectedLogCommitIndex, facade.Log.ReadCommitIndexTest());
        Assert.Equal(expectedLogCommitIndex, facade.Log.CommitIndex);
    }

    private static bool EqualityComparison(LogEntry left, LogEntry right) => Comparer.Equals(left, right);
}