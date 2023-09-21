using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using System.Text;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using FluentAssertions;
using Serilog.Core;
using TaskFlux.Core;
using Constants = Consensus.Raft.Persistence.Constants;

// ReSharper disable UseUtf8StringLiteral
// ReSharper disable StringLiteralTypo

namespace Consensus.Storage.Tests;

[Trait("Category", "Raft")]
public class StoragePersistenceFacadeTests
{
    private static LogEntry EmptyEntry(int term) => new(new Term(term), Array.Empty<byte>());

    private record ConsensusFileSystem(IFileInfo LogFile,
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

        return new ConsensusFileSystem(log, metadata, snapshot, tempDirectory);
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
        var logStorage = new FileLogStorage(fs.LogFile, fs.TemporaryDirectory);

        var term = initialTerm is null
                       ? DefaultTerm
                       : new Term(initialTerm.Value);
        var metadataStorage = new FileMetadataStorage(fs.MetadataFile.Open(FileMode.OpenOrCreate), term, votedFor);

        var snapshotStorage = new FileSystemSnapshotStorage(fs.SnapshotFile, fs.TemporaryDirectory, Logger.None);
        var facade = new StoragePersistenceFacade(logStorage, metadataStorage, snapshotStorage);

        return ( facade, fs );
    }

    private static readonly LogEntryEqualityComparer Comparer = new();

    [Fact]
    public void AppendBuffer__СПустымЛогом__ДолженДобавитьЗаписьВБуферВПамяти()
    {
        var (facade, _) = CreateFacade();
        var entry = new LogEntry(new Term(2), Array.Empty<byte>());

        facade.AppendBuffer(entry);

        var buffer = facade.ReadLogBufferTest();
        var actualEntry = buffer.Single();
        Assert.Equal(entry, actualEntry, Comparer);
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
    public void AppendBuffer__КогдаЕстьСнапшот__ДолженВернутьПравильныйНовыйИндекс(int logSize, int bufferSize)
    {
        var (facade, _) = CreateFacade();
        var lastSnapshotIndex = 10;
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(2), lastSnapshotIndex,
            new StubSnapshot(Array.Empty<byte>()));
        facade.SetupBufferTest(Enumerable.Range(0, bufferSize).Select(_ => RandomDataEntry(2)));
        facade.LogStorage.SetFileTest(Enumerable.Range(0, logSize).Select(_ => RandomDataEntry(2)));
        var expected = new LogEntryInfo(new Term(2), lastSnapshotIndex + logSize + bufferSize + 1);

        var actual = facade.AppendBuffer(new LogEntry(new Term(2), Array.Empty<byte>()));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void AppendBuffer__СПустымЛогом__НеДолженЗаписыватьЗаписьВLogStorage()
    {
        var (facade, _) = CreateFacade();
        var entry = new LogEntry(new Term(2), Array.Empty<byte>());

        facade.AppendBuffer(entry);

        Assert.Empty(facade.LogStorage.ReadAllTest());
    }

    [Fact]
    public void AppendBuffer__СПустымЛогом__ДолженВернутьАктуальнуюИнформациюОЗаписанномЭлементе()
    {
        var entry = new LogEntry(DefaultTerm, new byte[] {123, 4, 56});
        var (facade, _) = CreateFacade();
        // Индексирование начинается с 0
        var expected = new LogEntryInfo(entry.Term, 0);

        var actual = facade.AppendBuffer(entry);

        Assert.Equal(actual, expected);
        Assert.Equal(expected, facade.LastEntry);
    }

    [Fact]
    public void AppendBuffer__КогдаВБуфереЕстьЭлементы__ДолженВернутьПравильнуюЗапись()
    {
        var (facade, _) = CreateFacade(2);
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
        new(new Term(term), Encoding.UTF8.GetBytes(data));

    [Fact]
    public void AppendBuffer__КогдаБуферПустНоВХранилищеЕстьЭлементы__ДолженВернутьПравильнуюЗапись()
    {
        var (facade, _) = CreateFacade(2);
        facade.LogStorage.AppendRange(new[] {Entry(1, 99, 76, 33), Entry(1, 9), Entry(2, 94, 22, 48)});
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
    public void AppendBuffer__КогдаВБуфереИХранилищеЕстьЭлементы__ДолженВернутьПравильнуюЗапись()
    {
        var (facade, _) = CreateFacade(3);
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
        var (facade, _) = CreateFacade(entriesCount);
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
        var (facade, _) = CreateFacade(elementsCount + 1);
        var entries = Enumerable.Range(1, elementsCount)
                                .Select(RandomDataEntry)
                                .ToArray();

        facade.InsertBufferRange(entries, 0);

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

        facade.InsertBufferRange(toInsert, bufferSize);

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

        facade.InsertBufferRange(toInsert, insertIndex);

        var actual = facade.ReadLogBufferTest();

        Assert.Equal(expected, actual, Comparer);
        Assert.Empty(facade.ReadLogFileTest());
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
        facade.LogStorage.SetFileTest(logEntries);
        facade.SetLastApplied(3);

        facade.SaveSnapshot(new StubSnapshot(data));

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        Assert.True(fs.SnapshotFile.Exists);
        Assert.Equal(expectedLastEntry.Index, actualIndex);
        Assert.Equal(expectedLastEntry.Term, actualTerm);
        Assert.Equal(data, actualData);
        Assert.Equal(expectedLastEntry, facade.SnapshotStorage.LastLogEntry);
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
        facade.LogStorage.SetFileTest(logEntries);
        facade.SetLastApplied(4);

        facade.SaveSnapshot(new StubSnapshot(data));

        var expectedLastEntry = new LogEntryInfo(new Term(5), 4);
        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        Assert.Equal(expectedLastEntry.Index, actualIndex);
        Assert.Equal(expectedLastEntry.Term, actualTerm);
        Assert.Equal(data, actualData);
        Assert.Equal(expectedLastEntry, facade.SnapshotStorage.LastLogEntry);
    }

    [Fact]
    public void InstallSnapshot__КогдаЛогБылПустым__ДолженОбновитьИндексПоследнейПримененнойКоманды()
    {
        var (facade, _) = CreateFacade();
        var lastLogEntry = new LogEntryInfo(new Term(7), 10);
        var snapshotData = RandomBytes(0);

        facade.InstallSnapshot(lastLogEntry, new StubSnapshot(snapshotData))
              .EnumerateAll();

        var (actualLastIndex, actualLastTerm, actualData) = facade.ReadSnapshotFileTest();

        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.SnapshotStorage.LastLogEntry
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");

        facade.GetLastAppliedIndexTest()
              .Should()
              .Be(lastLogEntry.Index, "лог был пустым и новый снапшот установлен");
    }

    [Fact]
    public void InstallSnapshot__КогдаВЛогеБылиКоманды__ДолженВернутьПравильныеНепримененныеКоманды()
    {
        var (facade, _) = CreateFacade();
        var lastLogEntry = new LogEntryInfo(new Term(7), 10);
        var snapshotData = RandomBytes(0);

        facade.InstallSnapshot(lastLogEntry, new StubSnapshot(snapshotData))
              .EnumerateAll();

        var (actualLastIndex, actualLastTerm, actualData) = facade.ReadSnapshotFileTest();

        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.SnapshotStorage.LastLogEntry
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");

        facade.GetLastAppliedIndexTest()
              .Should()
              .Be(lastLogEntry.Index, "лог был пустым и новый снапшот установлен");
    }

    [Fact]
    public void InstallSnapshot__КогдаФайлСнапшотаСуществовалНеПустой__ДолженСохранитьДанныеСнапшота()
    {
        var (facade, _) = CreateFacade();
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(4), 3, new StubSnapshot(new byte[] {1, 2, 3}));
        var lastLogEntry = new LogEntryInfo(new Term(7), 10);
        var snapshotData = RandomBytes(123);

        facade.InstallSnapshot(lastLogEntry, new StubSnapshot(snapshotData))
              .EnumerateAll();

        var (actualLastIndex, actualLastTerm, actualData) = facade.ReadSnapshotFileTest();

        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.SnapshotStorage.LastLogEntry
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");
    }

    [Fact]
    public void InstallSnapshot__КогдаФайлаСнапшотаНеСуществовало__ДолженСохранитьДанныеСнапшота()
    {
        // Должен удалить предшествующие записи в логе
        var (facade, _) = CreateFacade();

        var lastLogEntry = new LogEntryInfo(new Term(1), 10);
        var snapshotData = RandomBytes(123);
        facade.InstallSnapshot(lastLogEntry, new StubSnapshot(snapshotData))
              .EnumerateAll();

        var (actualLastIndex, actualLastTerm, actualData) = facade.ReadSnapshotFileTest();

        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.SnapshotStorage.LastLogEntry
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
        facade.LogStorage.SetFileTest(existingLog);
        // В снапшоте - все команды до 4-ой (индекс 3)
        var lastLogEntry = new LogEntryInfo(new Term(3), 3);
        var expectedLog = existingLog[4..];

        facade.InstallSnapshot(lastLogEntry, new StubSnapshot(snapshotData))
              .EnumerateAll();

        // Проверка корректности общей работы
        var (actualLastIndex, actualLastTerm, actualData) = facade.ReadSnapshotFileTest();
        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.SnapshotStorage.LastLogEntry
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");

        // Проверка корректности обновления лога
        var actualLog = facade.ReadLogFileTest();
        actualLog
           .Should()
           .Equal(expectedLog, LogEntryComparisonFunc, "файл лога должен очиститься, до указанной команды");
    }

    [Fact]
    public void InstallSnapshot__КогдаВБуфереКомандБылиПересекающиесяКоманды__ДолженОчиститьБуферДоТребуемогоИндекса()
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

        var existingBuffer = new[]
        {
            RandomDataEntry(3), // 5
            RandomDataEntry(4), // 6
            RandomDataEntry(5), // 7
            RandomDataEntry(5), // 8
            RandomDataEntry(5), // 9
        };
        facade.LogStorage.SetFileTest(existingLog);
        facade.SetupBufferTest(existingBuffer);
        // В снапшоте - все команды до 4-ой (индекс 3)
        var lastLogEntry = new LogEntryInfo(new Term(5), 8);
        // Индексирование в буфере будет с 3
        var expectedBuffer = existingBuffer[3..];

        facade.InstallSnapshot(lastLogEntry, new StubSnapshot(snapshotData))
              .EnumerateAll();

        // Проверка корректности общей работы
        var (actualLastIndex, actualLastTerm, actualData) = facade.ReadSnapshotFileTest();
        actualLastIndex
           .Should()
           .Be(lastLogEntry.Index, "указанный индекс должен сохраниться в файл снапшота");
        actualLastTerm
           .Should()
           .Be(lastLogEntry.Term, "указанный терм должен сохранитсья в файл снапшота");
        actualData
           .Should()
           .Equal(snapshotData, "записанные данные должны быть равны передаваемым");
        facade.SnapshotStorage.LastLogEntry
              .Should()
              .Be(lastLogEntry, "свойство должно быть равным такому же как и в снапшоте");

        // Проверка корректности обновления лога
        var actualLog = facade.ReadLogFileTest();
        actualLog
           .Should()
           .BeEmpty("все команды в логе должны удалиться");
        var actualBuffer = facade.ReadLogBufferTest();
        actualBuffer
           .Should()
           .Equal(expectedBuffer, LogEntryComparisonFunc, "команды в буфере должны удалиться до нужного количества");
    }

    // TODO: тесты на применение оставшихся команд
    // TODO: тесты на обновление состояния после установки снапшота

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
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(2), 3, new StubSnapshot(oldData));

        // У нас в логе 4 команды, причем применены все
        var lastTerm = new Term(5);
        var exisingLogData = new[]
        {
            RandomDataEntry(3),              // 4
            RandomDataEntry(3),              // 5
            RandomDataEntry(4),              // 6
            RandomDataEntry(lastTerm.Value), // 7
        };
        facade.LogStorage.SetFileTest(exisingLogData);
        // Все команды применены из лога
        facade.SetLastApplied(7);

        facade.SaveSnapshot(new StubSnapshot(newSnapshotData));

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        var expectedIndex = 7;
        var expectedTerm = lastTerm;
        Assert.Equal(expectedIndex, actualIndex);
        Assert.Equal(expectedTerm, actualTerm);
        Assert.Equal(newSnapshotData, actualData);
        Assert.Equal(new LogEntryInfo(expectedTerm, expectedIndex), facade.SnapshotStorage.LastLogEntry);
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
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(2), 3, new StubSnapshot(oldData));
        facade.UpdateState(currentTerm, null);

        // У нас в логе 4 команды, причем применены все
        var exisingLogData = new[]
        {
            RandomDataEntry(3), // 4
            RandomDataEntry(3), // 5
            RandomDataEntry(4), // 6
            RandomDataEntry(5), // 7
        };
        facade.LogStorage.SetFileTest(exisingLogData);
        // Все команды применены из лога
        facade.SetLastApplied(5);

        facade.SaveSnapshot(new StubSnapshot(newSnapshotData));

        var (actualIndex, actualTerm, actualData) = facade.SnapshotStorage.ReadAllDataTest();
        var expectedIndex = 5;
        var expectedTerm = new Term(3);
        Assert.Equal(expectedIndex, actualIndex);
        Assert.Equal(expectedTerm, actualTerm);
        Assert.Equal(newSnapshotData, actualData);
        Assert.Equal(new LogEntryInfo(expectedTerm, expectedIndex), facade.SnapshotStorage.LastLogEntry);
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
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(1), snapshotLastIndex, NullSnapshot);

        var success = facade.TryGetFrom(index, out _);
        Assert.False(success);
    }

    [Fact]
    public void
        GetPrecedingEntryInfo__КогдаСуществуетСнапшот_ИндексРавенПервомуВЛоге__ДолженВернутьВхождениеИзСнапшота()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.WriteSnapshotDataTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);

        var actual = facade.GetPrecedingEntryInfo(snapshotLastEntry.Index + 1);

        Assert.Equal(snapshotLastEntry, actual);
    }

    [Fact]
    public void
        GetPrecedingEntryInfo__КогдаСуществуетСнапшот_ИндексНаходитсяВПределахЛога__ДолженВернутьВхождениеИзЛога()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.WriteSnapshotDataTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var entries = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        facade.LogStorage.SetFileTest(entries);
        var expected = new LogEntryInfo(new Term(2), 12);

        var actual = facade.GetPrecedingEntryInfo(13);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void GetPrecedingEntryInfo__КогдаСуществуетСнапшот_ИндексПервыйИзБуфера__ДолженВернутьВхождениеИзЛога()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.WriteSnapshotDataTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var entries = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        facade.LogStorage.SetFileTest(entries);
        var expected = new LogEntryInfo(new Term(3), 13);

        var actual = facade.GetPrecedingEntryInfo(14);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void GetPrecedingEntryInfo__КогдаСуществуетСнапшот_ИндексВПределахБуфера__ДолженВернутьВхождениеИзБуфера()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.WriteSnapshotDataTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var entries = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        var buffer = new[]
        {
            RandomDataEntry(3), // 14
            RandomDataEntry(3), // 15
            RandomDataEntry(4), // 16
        };
        facade.SetupBufferTest(buffer);
        facade.LogStorage.SetFileTest(entries);
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
        facade.SnapshotStorage.WriteSnapshotDataTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var entries = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        var buffer = new[]
        {
            RandomDataEntry(3), // 14
            RandomDataEntry(3), // 15
            RandomDataEntry(4), // 16
        };
        facade.SetupBufferTest(buffer);
        facade.LogStorage.SetFileTest(entries);
        var expected = new LogEntryInfo(new Term(4), 16);

        var actual = facade.GetPrecedingEntryInfo(17);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void
        GetPrecedingEntryInfo__КогдаСуществуетСнапшот_БуферПуст_ИндексПослеИндексаСнапшота__ДолженВернутьВхождениеИзСнапшота()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastEntry = new LogEntryInfo(new Term(2), 10);
        facade.SnapshotStorage.WriteSnapshotDataTest(snapshotLastEntry.Term, snapshotLastEntry.Index, NullSnapshot);
        var buffer = new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
        };
        facade.SetupBufferTest(buffer);
        var expected = new LogEntryInfo(new Term(2), 10);

        var actual = facade.GetPrecedingEntryInfo(11);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    public void InsertBufferRange__КогдаЕстьСнашпот__ДолженДобавитьЗаписиВКонец(int initialBufferSize)
    {
        var (facade, _) = CreateFacade();
        var snapshotLastIndex = 11;
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(3), snapshotLastIndex,
            new StubSnapshot(Array.Empty<byte>()));
        var existingBuffer = Enumerable.Range(0, initialBufferSize)
                                       .Select(_ => RandomDataEntry(3))
                                       .ToArray();
        facade.SetupBufferTest(existingBuffer);
        var toInsert = new[] {RandomDataEntry(3), RandomDataEntry(3),};
        var expected = existingBuffer.Concat(toInsert)
                                     .ToArray();

        facade.InsertBufferRange(toInsert, snapshotLastIndex + initialBufferSize + 1);

        var actual = facade.ReadLogBufferTest();

        actual.Should()
              .Equal(expected, "записи должны сконкатенироваться");
    }

    [Fact]
    public void InsertBufferRange__КогдаЕстьСнапшот__ДолженПерезаписатьБуфер()
    {
        var (facade, _) = CreateFacade();
        var snapshotLastIndex = 11;
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(3), snapshotLastIndex,
            new StubSnapshot(Array.Empty<byte>()));
        var existingBuffer = new[]
        {
            RandomDataEntry(3), // 11
            RandomDataEntry(4), // 12
            RandomDataEntry(4), // 13
            RandomDataEntry(4), // 14
        };
        facade.SetupBufferTest(existingBuffer);
        var toInsert = new[] {RandomDataEntry(3), RandomDataEntry(3),};

        var expected = existingBuffer[..0]
                      .Concat(toInsert)
                      .ToArray();

        facade.InsertBufferRange(toInsert, 12);

        var actual = facade.ReadLogBufferTest();

        actual.Should()
              .Equal(expected, "записи должны сконкатенироваться");
    }

    [Fact]
    public void SaveSnapshot__КогдаСнапшотУжеСуществовал__ДолженПерезаписатьФайл()
    {
        var (facade, _) = CreateFacade();
        var oldSnapshot = new StubSnapshot(RandomBytes(100));
        var newSnapshot = new StubSnapshot(RandomBytes(90));
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(2), 10, oldSnapshot);
        facade.LogStorage.SetFileTest(new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(3), // 12
        });
        facade.SetLastApplied(12);

        facade.SaveSnapshot(newSnapshot);

        var (actualIndex, actualTerm, actualData) = facade.ReadSnapshotFileTest();
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
    public void GetNotApplied__КогдаНикакиеЗаписиНеБылиПрименены()
    {
        var (facade, _) = CreateFacade();
        var log = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(2), // 2
            RandomDataEntry(3), // 3
        };
        var buffer = new[]
        {
            RandomDataEntry(4), // 4
        };
        facade.SetupBufferTest(buffer);
        facade.LogStorage.SetFileTest(log);

        var actual = facade.GetNotApplied();

        actual.Should()
              .Equal(log, LogEntryComparisonFunc, "никакие записи не были применены, надо вернуть весь лог");
    }

    [Fact]
    public void GetNotApplied__КогдаЧастьБылаПрименена()
    {
        var (facade, _) = CreateFacade();
        var log = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(2), // 2
            RandomDataEntry(3), // 3
        };
        var buffer = new[]
        {
            RandomDataEntry(4), // 4
        };
        facade.SetupBufferTest(buffer);
        facade.LogStorage.SetFileTest(log);
        facade.SetLastApplied(2);
        var expected = log[3..];

        var actual = facade.GetNotApplied();

        actual.Should()
              .Equal(expected, equalityComparison: LogEntryComparisonFunc,
                   because: "записи до 3 индекса были применены");
    }

    [Fact]
    public void GetNotApplied__КогдаВсеБылиПрименены__ДолженВернутьПустойСписок()
    {
        var (facade, _) = CreateFacade();
        var log = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(2), // 2
            RandomDataEntry(3), // 3
        };
        var buffer = new[]
        {
            RandomDataEntry(4), // 4
        };
        facade.SetupBufferTest(buffer);
        facade.LogStorage.SetFileTest(log);
        facade.SetLastApplied(3);

        var actual = facade.GetNotApplied();

        actual.Should()
              .BeEmpty("все записи из лога были закоммичены");
    }

    [Fact]
    public void GetNotApplied__КогдаЕстьСнапшот_ВесьЛогНеПрименен__ДолженВернутьВесьЛог()
    {
        var (facade, _) = CreateFacade();
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(2), 10, new StubSnapshot(Array.Empty<byte>()));
        var log = new[]
        {
            RandomDataEntry(1), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(2), // 13
            RandomDataEntry(3), // 14
        };
        var buffer = new[]
        {
            RandomDataEntry(4), // 15
        };
        facade.SetupBufferTest(buffer);
        facade.LogStorage.SetFileTest(log);

        var actual = facade.GetNotApplied();

        actual.Should()
              .Equal(log, LogEntryComparisonFunc, "никакие записи из лога не были применены, надо вернуть весь лог");
    }

    [Fact]
    public void GetNotApplied__КогдаЕстьСнапшот_ЧастьЛогаНеПрименена__ДолженВернутьЧастьЛога()
    {
        var (facade, _) = CreateFacade();
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(2), 10, new StubSnapshot(Array.Empty<byte>()));
        var log = new[]
        {
            RandomDataEntry(1), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(2), // 13
            RandomDataEntry(3), // 14
        };
        var buffer = new[]
        {
            RandomDataEntry(4), // 15
        };
        facade.SetupBufferTest(buffer);
        facade.LogStorage.SetFileTest(log);
        facade.SetLastApplied(12);
        var expected = log[2..];

        var actual = facade.GetNotApplied();

        actual.Should()
              .Equal(expected, LogEntryComparisonFunc, "записи до 12 (локального 2) были применены уже");
    }

    [Fact]
    public void SaveSnapshot__КогдаИндексПримененнойКомандыРавенПоследнемуЗакоммиченному__ДолженОчиститьЛог()
    {
        var (facade, _) = CreateFacade();
        var oldSnapshot = new StubSnapshot(RandomBytes(100));
        var newSnapshot = new StubSnapshot(RandomBytes(90));
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(2), 10, oldSnapshot);
        facade.LogStorage.SetFileTest(new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(3), // 12
        });
        facade.SetLastApplied(12);

        facade.SaveSnapshot(newSnapshot);

        var actualLogFile = facade.ReadLogFileTest();
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
        facade.SnapshotStorage.WriteSnapshotDataTest(new Term(2), 10, oldSnapshot);
        var lastLogEntry = RandomDataEntry(3);
        var expected = new[] {lastLogEntry};
        facade.LogStorage.SetFileTest(new[]
        {
            RandomDataEntry(2), // 11
            RandomDataEntry(2), // 12
            RandomDataEntry(3), // 13
            lastLogEntry,       // 14
        });
        facade.SetLastApplied(13);

        facade.SaveSnapshot(newSnapshot);

        var actualLogFile = facade.ReadLogFileTest();
        actualLogFile.Should()
                     .Equal(expected, EqualityComparison,
                          "последняя примененная запись имеет предпоследний индекс в логе");
    }

    private static bool EqualityComparison(LogEntry left, LogEntry right) => Comparer.Equals(left, right);
}

file static class EnumerableExtensions
{
    public static void EnumerateAll<T>(this IEnumerable<T> data)
    {
        foreach (var _ in data)
        {
            /*  */
        }
    }
}