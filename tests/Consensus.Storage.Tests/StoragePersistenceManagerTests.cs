using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using System.Text;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using TaskFlux.Core;

namespace Consensus.Storage.Tests;

[Trait("Category", "Raft")]
public class StoragePersistenceManagerTests
{
    private static LogEntry EmptyEntry(int term) => new(new Term(term), Array.Empty<byte>());

    private record ConsensusFileSystem(MockFileSystem Fs,
                                       IFileInfo LogFile,
                                       IFileInfo MetadataFile,
                                       IFileInfo SnapshotFile,
                                       IDirectoryInfo TemporaryDirectory);

    private static readonly string BaseDirectory = Path.Combine("var", "lib", "taskflux");
    private static readonly string ConsensusDirectory = Path.Combine(BaseDirectory, "consensus");

    private static ConsensusFileSystem CreateFileSystem()
    {
        var fs = new MockFileSystem(new Dictionary<string, MockFileData>()
        {
            [ConsensusDirectory] = new MockDirectoryData()
        });

        var log = fs.FileInfo.New(Path.Combine(ConsensusDirectory, Constants.LogFileName));
        var metadata = fs.FileInfo.New(Path.Combine(ConsensusDirectory, Constants.MetadataFileName));
        var snapshot = fs.FileInfo.New(Path.Combine(ConsensusDirectory, Constants.SnapshotFileName));
        var tempDirectory = fs.DirectoryInfo.New(Path.Combine(ConsensusDirectory, "temporary"));

        log.Create();
        metadata.Create();
        snapshot.Create();
        tempDirectory.Create();

        return new ConsensusFileSystem(fs, log, metadata, snapshot, tempDirectory);
    }

    private static readonly Term DefaultTerm = Term.Start;

    /// <summary>
    /// Метод для создания фасада с файлами в памяти.
    /// Создает и инициализирует нужную структуру файлов в памяти.
    /// </summary>
    /// <remarks>Создаваемые файлы пустые</remarks>
    private static (StoragePersistenceFacade Facade, ConsensusFileSystem Fs) CreateFacade(
        int? initialTerm = null,
        NodeId? votedFor = null)
    {
        var fs = CreateFileSystem();
        var fileLogStream = fs.LogFile.Open(FileMode.OpenOrCreate);
        var logStorage = new FileLogStorage(fileLogStream);

        var term = initialTerm is null
                       ? DefaultTerm
                       : new Term(initialTerm.Value);
        var metadataStorage = new FileMetadataStorage(fs.MetadataFile.Open(FileMode.OpenOrCreate), term, votedFor);

        var snapshotStorage = new FileSystemSnapshotStorage(fs.SnapshotFile, fs.TemporaryDirectory);
        var facade = new StoragePersistenceFacade(logStorage, metadataStorage, snapshotStorage);

        return ( facade, fs );
    }

    private static readonly LogEntryEqualityComparer Comparer = new();

    [Fact]
    public void Append__СПустымЛогом__ДолженДобавитьЗаписьВБуферВПамяти()
    {
        var (facade, _) = CreateFacade();
        var entry = new LogEntry(new Term(2), Array.Empty<byte>());

        facade.AppendBuffer(entry);

        var buffer = facade.ReadLogBufferTest();
        var actualEntry = buffer.Single();
        Assert.Equal(entry, actualEntry, Comparer);
    }

    [Fact]
    public void Append__СПустымЛогом__НеДолженЗаписыватьЗаписьВLogStorage()
    {
        var (facade, _) = CreateFacade();
        var entry = new LogEntry(new Term(2), Array.Empty<byte>());

        facade.AppendBuffer(entry);

        Assert.Empty(facade.LogStorage.ReadAllTest());
    }

    [Fact]
    public void Append__СПустымЛогом__ДолженВернутьАктуальнуюИнформациюОЗаписанномЭлементе()
    {
        var entry = new LogEntry(DefaultTerm, new byte[] {123, 4, 56});
        var (facade, fs) = CreateFacade();
        // Индексирование начинается с 0
        var expected = new LogEntryInfo(entry.Term, 0);

        var actual = facade.AppendBuffer(entry);

        Assert.Equal(actual, expected);
        Assert.Equal(expected, facade.LastEntry);
    }

    [Fact]
    public void Append__КогдаВБуфереЕстьЭлементы__ДолженВернутьПравильнуюЗапись()
    {
        var (facade, fs) = CreateFacade(2);
        var buffer = new List<LogEntry>()
        {
            new(new Term(1), new byte[] {1, 2, 3}), new(new Term(2), new byte[] {4, 5, 6}),
        };
        facade.SetupBufferTest(buffer);
        var entry = new LogEntry(new Term(3), new byte[] {7, 8, 9});
        var expected = new LogEntryInfo(entry.Term, 2);

        var actual = facade.AppendBuffer(entry);

        Assert.Equal(expected, actual);
        Assert.Equal(expected, facade.LastEntry);
    }

    private static LogEntry Entry(int term, params byte[] data) => new LogEntry(new Term(term), data);

    private static LogEntry Entry(int term, string data = "") =>
        new LogEntry(new Term(term), Encoding.UTF8.GetBytes(data));

    [Fact]
    public void Append__КогдаБуферПустНоВХранилищеЕстьЭлементы__ДолженВернутьПравильнуюЗапись()
    {
        var (facade, fs) = CreateFacade(2);
        facade.LogStorage.AppendRange(new LogEntry[] {Entry(1, 99, 76, 33), Entry(1, 9), Entry(2, 94, 22, 48)});
        var entry = Entry(2, 4, 1, 34);
        var expected = new LogEntryInfo(entry.Term, 3);

        var actual = facade.AppendBuffer(entry);

        Assert.Equal(expected, actual);
        Assert.Equal(expected, facade.LastEntry);
        var stored = facade.ReadLogBufferTest();
        Assert.Single(stored);
        Assert.Equal(entry, stored.Single(), Comparer);
    }

    [Fact]
    public void Append__КогдаВБуфереИХранилищеЕстьЭлементы__ДолженВернутьПравильнуюЗапись()
    {
        var (facade, fs) = CreateFacade(3);
        facade.LogStorage.AppendRange(new[] {Entry(1, "adfasfas"), Entry(2, "aaaa"), Entry(2, "aegqer89987")});

        facade.SetupBufferTest(new List<LogEntry>() {Entry(3, "asdf"),});

        var entry = Entry(3, "data");
        var expected = new LogEntryInfo(entry.Term, 4);

        var actual = facade.AppendBuffer(entry);

        Assert.Equal(expected, actual);
        Assert.Equal(expected, facade.LastEntry);
        Assert.Equal(entry, facade.ReadLogBufferTest()[^1]);
        Assert.DoesNotContain(facade.LogStorage.ReadAllTest(), e => Comparer.Equals(e, entry));
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
    public void Commit__СЭлементамиВБуфере__ДолженЗаписатьЗаписиВLogStorage(int entriesCount, int commitIndex)
    {
        // На всякий случай выставим терм в количество элементов (термы инкрементируются)
        var (facade, fs) = CreateFacade(entriesCount);
        var bufferElements = Enumerable.Range(1, entriesCount)
                                       .Select(RandomDataEntry)
                                       .ToList();

        var (expectedLog, expectedBuffer) = Split(bufferElements, commitIndex);
        facade.SetupBufferTest(bufferElements);

        facade.Commit(commitIndex);

        Assert.Equal(expectedBuffer, facade.ReadLogBufferTest(), Comparer);
        Assert.Equal(expectedLog, facade.LogStorage.ReadAllTest(), Comparer);
        Assert.Equal(commitIndex, facade.CommitIndex);
    }

    [Fact]
    public void TryGetFrom__КогдаЗаписиВПамяти__ДолженВернутьТребуемыеЗаписи()
    {
        var (facade, _) = CreateFacade(5);
        // 4 записи в буфере с указанными индексами (глобальными)
        var bufferEntries = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(3), // 2
            RandomDataEntry(4), // 3
        };

        facade.SetupBufferTest(bufferEntries);

        var index = 2;
        var expected = bufferEntries[index..];

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
        var (log, buffer) = Split(entries, logEndIndex);
        var (facade, _) = CreateFacade(entriesCount);

        facade.SetupBufferTest(buffer);
        facade.LogStorage.AppendRange(log);

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
        var buffer = Enumerable.Range(1, bufferSize)
                               .Select(RandomDataEntry)
                               .ToArray();
        facade.SetupBufferTest(buffer);
        var expected = GetExpected();

        var actual = facade.GetPrecedingEntryInfo(entryIndex);

        Assert.Equal(expected, actual);

        LogEntryInfo GetExpected()
        {
            var e = buffer[entryIndex - 1];
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

        facade.LogStorage.AppendRange(entries);
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
        var (log, buffer) = Split(entries, logEndIndex);

        facade.SetupBufferTest(buffer);
        facade.LogStorage.AppendRange(log);

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
    public void InsertRange__СПустымБуфером__ДолженДобавитьЗаписиВБуфер(int elementsCount)
    {
        var (facade, fs) = CreateFacade(elementsCount + 1);
        var entries = Enumerable.Range(1, elementsCount)
                                .Select(RandomDataEntry)
                                .ToArray();

        facade.InsertRange(entries, 0);

        Assert.Equal(entries, facade.ReadLogBufferTest());
        Assert.Empty(facade.LogStorage.ReadAllTest());
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
        var buffer = Enumerable.Range(1, bufferSize)
                               .Select(RandomDataEntry)
                               .ToList();

        var toInsert = Enumerable.Range(bufferSize + 1, toInsertSize)
                                 .Select(RandomDataEntry)
                                 .ToList();

        var expected = buffer.Concat(toInsert)
                             .ToList();

        var (facade, _) = CreateFacade(bufferSize + toInsertSize + 1);
        facade.SetupBufferTest(buffer);

        facade.InsertRange(toInsert, bufferSize);

        var actual = facade.ReadLogBufferTest();
        Assert.Equal(expected, actual, Comparer);
        Assert.Empty(facade.ReadLogFileTest());
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
        var buffer = Enumerable.Range(1, bufferCount)
                               .Select(EmptyEntry)
                               .ToList();

        var toInsert = Enumerable.Range(bufferCount + 1, toInsertCount)
                                 .Select(EmptyEntry)
                                 .ToList();

        var expected = buffer.Take(insertIndex)
                             .Concat(toInsert)
                             .ToList();

        var (facade, _) = CreateFacade(bufferCount + toInsertCount + 1);
        facade.SetupBufferTest(buffer);

        facade.InsertRange(toInsert, insertIndex);

        var actual = facade.ReadLogBufferTest();

        Assert.Equal(expected, actual, Comparer);
        Assert.Empty(facade.ReadLogFileTest());
    }

    [Fact]
    public void SaveSnapshot__КогдаФайлаСнапшотаНеБыло__ДолженСоздатьНовыйФайлСнапшота()
    {
        var (facade, fs) = CreateFacade();

        var entry = new LogEntryInfo(new Term(1), 1);
        var data = new byte[] {1, 2, 3};

        facade.SaveSnapshot(entry, new StubSnapshot(data));

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllData();
        Assert.True(fs.SnapshotFile.Exists);
        Assert.Equal(entry.Index, actualIndex);
        Assert.Equal(entry.Term, actualTerm);
        Assert.Equal(data, actualData);
        Assert.Equal(entry, facade.SnapshotStorage.LastLogEntry);
    }

    [Fact]
    public void SaveSnapshot__КогдаФайлСнапшотаСуществовалПустой__ДолженПерезаписатьСтарыйФайл()
    {
        var entry = new LogEntryInfo(new Term(1), 1);
        var data = new byte[128];
        Random.Shared.NextBytes(data);

        var (facade, _) = CreateFacade();
        facade.SaveSnapshot(entry, new StubSnapshot(data));

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllData();
        Assert.Equal(entry.Index, actualIndex);
        Assert.Equal(entry.Term, actualTerm);
        Assert.Equal(data, actualData);
        Assert.Equal(entry, facade.SnapshotStorage.LastLogEntry);
    }

    [Fact]
    public void SaveSnapshot__КогдаФайлСнапшотаСуществовалСДанными__ДолженПерезаписатьСтарыйФайл()
    {
        var entry = new LogEntryInfo(new Term(1), 1);

        var data = new byte[128];
        Random.Shared.NextBytes(data);

        var (facade, _) = CreateFacade();

        // Размер этих данных больше, чем новых
        var oldData = new byte[164];
        Random.Shared.NextBytes(oldData);
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(123), 222, new StubSnapshot(oldData));

        facade.SaveSnapshot(entry, new StubSnapshot(data));

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllData();
        Assert.Equal(entry.Index, actualIndex);
        Assert.Equal(entry.Term, actualTerm);
        Assert.Equal(data, actualData);
        Assert.Equal(entry, facade.SnapshotStorage.LastLogEntry);
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

        facade.SnapshotStorage.WriteSnapshotDataTest(term,
            snapshotLastIndex,
            new StubSnapshot(Array.Empty<byte>()));

        facade.LogStorage.SetFileTest(fileEntries);
        facade.SetupBufferTest(bufferEntries);

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

        facade.SnapshotStorage.WriteSnapshotDataTest(term,
            snapshotLastIndex,
            new StubSnapshot(Array.Empty<byte>()));

        facade.LogStorage.SetFileTest(fileEntries);
        facade.SetupBufferTest(bufferEntries);

        var expected = fileEntries
                      .Concat(bufferEntries)
                      .Skip(globalIndex - snapshotLastIndex - 1)
                      .ToArray();

        var success = facade.TryGetFrom(globalIndex, out var actual);

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
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(1), snapshotLastIndex, NullSnapshot);

        var success = facade.TryGetFrom(index, out _);
        Assert.False(success);
    }
}