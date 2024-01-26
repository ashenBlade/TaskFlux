using System.IO.Abstractions;
using System.Text;
using FluentAssertions;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Persistence.Log;
using TaskFlux.Consensus.Tests.Infrastructure;

namespace TaskFlux.Consensus.Tests;

[Trait("Category", "Persistence")]
public class FileLogTests : IDisposable
{
    private static LogEntry Entry(int term, string data)
        => new(new Term(term), Encoding.UTF8.GetBytes(data));

    private record MockFiles(IFileInfo LogFile, IDirectoryInfo TemporaryDirectory, IDirectoryInfo DataDirectory);

    /// <summary>
    /// Файлы, который создаются вызовом <see cref="CreateLog"/>.
    /// Это поле используется для проверки того, что после операции файл остается в корректном состоянии.
    /// </summary>
    private MockFiles? _createdFiles;

    private FileLog? _createdFileLog;

    public void Dispose()
    {
        if (_createdFiles is var (logFile, tempDir, dataDir))
        {
            var fileLogCreation = () => FileLog.Initialize(dataDir);
            fileLogCreation.Should()
                           .NotThrow("объект лога должен уметь создаваться после операций другого объекта лога");
        }

        if (_createdFileLog is not null)
        {
            var fileLogValidation = () => _createdFileLog.ValidateFileTest();
            fileLogValidation
               .Should()
               .NotThrow("после операций файл должен оставаться в консистентном состоянии");
        }
    }

    private (FileLog Log, MockFiles Mock) CreateLog()
    {
        var fileSystem = Helpers.CreateFileSystem();
        var mockFiles = new MockFiles(fileSystem.Log, fileSystem.TemporaryDirectory, fileSystem.DataDirectory);
        _createdFiles = mockFiles;
        var fileLog = FileLog.Initialize(fileSystem.DataDirectory);
        _createdFileLog = fileLog;
        return ( fileLog, mockFiles );
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

        storage.AppendRange(expected);

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

        storage.AppendRange(expected);

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
        firstLog.AppendRange(entries);

        var secondLog = FileLog.Initialize(fs.DataDirectory);
        var actual = secondLog.ReadAllTest();

        Assert.Equal(entries, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 2)]
    [InlineData(2, 1)]
    [InlineData(10, 5)]
    [InlineData(5, 10)]
    public void AppendRange__СНеПустымЛогомИИндексомКонца__ДолженДобавитьЗаписиВКонец(int initialSize, int appendSize)
    {
        var initial = Enumerable.Range(1, initialSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var appended = Enumerable.Range(1 + initialSize, appendSize)
                                 .Select(i => Entry(i, $"data {i}"))
                                 .ToArray();
        var expected = initial.Concat(appended).ToArray();

        var (log, _) = CreateLog();
        log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted: initial);

        log.AppendRange(appended);

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
    public void InsertRange__КогдаИндекс0ИВсеЗаписиНеЗакоммичены__ДолженЗаменитьСодержимоеПереданнымиЗаписями(
        int uncommittedCount,
        int insertCount)
    {
        var (log, _) = CreateLog();
        var uncommitted = CreateEntries(1, uncommittedCount);
        var toInsert = CreateEntries(1, insertCount);
        log.SetupLogTest(NoEntries, uncommitted);
        var expected = toInsert;

        log.InsertRangeOverwrite(toInsert, 0);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    private static readonly LogEntry[] NoEntries = Array.Empty<LogEntry>();

    private List<LogEntry> CreateEntries(int startTerm, int count)
    {
        return Enumerable.Range(startTerm, count)
                         .Select(i => Entry(i, i.ToString()))
                         .ToList();
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 10)]
    [InlineData(10, 1)]
    [InlineData(12, 22)]
    public void InsertRangeOverwrite__КогдаЕстьЗакоммиченныеЗаписиИИндексРавенРазмеруЛога__ДолженДобавитьЗаписиВКонец(
        int committedCount,
        int insertCount)
    {
        var (log, _) = CreateLog();
        var committed = CreateEntries(1, committedCount);
        var toInsert = CreateEntries(committedCount + 1, insertCount);
        log.SetupLogTest(committed, NoEntries);
        var expected = committed.Concat(toInsert).ToArray();

        log.InsertRangeOverwrite(toInsert, committedCount);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 1, 1)]
    [InlineData(1, 2, 1)]
    [InlineData(1, 2, 2)]
    [InlineData(10, 3, 1)]
    [InlineData(2, 5, 10)]
    public void
        InsertRangeOverwrite__КогдаЕстьЗакоммиченныеИНеЗакоммиченныеЗаписиИИндексРавенРазмеруЛога__ДолженДобавитьЗаписиВКонец(
        int committedCount,
        int uncommittedCount,
        int insertCount)
    {
        var (log, _) = CreateLog();
        var committed = CreateEntries(1, committedCount);
        var uncommitted = CreateEntries(committedCount + 1, uncommittedCount);
        var toInsert = CreateEntries(committedCount + uncommittedCount + 1, insertCount);
        log.SetupLogTest(committed, uncommitted);
        var expected = committed.Concat(uncommitted).Concat(toInsert).ToArray();

        log.InsertRangeOverwrite(toInsert, committedCount + uncommittedCount);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 1, 1)]
    [InlineData(1, 2, 2)]
    [InlineData(1, 2, 1)]
    [InlineData(10, 5, 4)]
    [InlineData(10, 5, 5)]
    [InlineData(10, 5, 6)]
    public void
        InsertRangeOverwrite__КогдаЕстьЗакоммиченныеИНеЗакоммиченныеЗаписиИИндексРавенКоличествуЗакоммиченных__ДолженЗатеретьНеЗакоммиченныеЗаписи(
        int committedCount,
        int uncommittedCount,
        int insertCount)
    {
        var (log, _) = CreateLog();
        var committed = CreateEntries(1, committedCount);
        var uncommitted = CreateEntries(committedCount + 1, uncommittedCount);
        var toInsert = CreateEntries(committedCount + uncommittedCount + 1, insertCount);
        log.SetupLogTest(committed, uncommitted);
        var expected = committed.Concat(toInsert).ToArray();

        log.InsertRangeOverwrite(toInsert, committedCount);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 1, 1, 1)]
    [InlineData(1, 2, 2, 2)]
    [InlineData(2, 2, 3, 2)]
    [InlineData(10, 5, 10, 4)]
    [InlineData(10, 5, 13, 10)]
    [InlineData(2, 0, 2, 10)]
    public void InsertRangeOverwrite__КогдаЗаписиДобавленыВНачало__НеДолженОбновлятьИндексЗакоммиченныхЗаписей(
        int committedCount,
        int uncommittedCount,
        int insertIndex,
        int insertCount)
    {
        var (log, _) = CreateLog();
        var committed = CreateEntries(1, committedCount);
        var uncommitted = CreateEntries(committedCount + 1, uncommittedCount);
        var toInsert = CreateEntries(committedCount + uncommittedCount + 1, insertCount);
        log.SetupLogTest(committed, uncommitted);
        var oldCommitIndex = log.CommitIndex;

        log.InsertRangeOverwrite(toInsert, insertIndex);

        Assert.Equal(oldCommitIndex, log.CommitIndex);
        Assert.Equal(oldCommitIndex, log.ReadCommitIndexTest());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public void AppendИCommit__КогдаЧередуются__ДолжныДобавитьЗаписиВЛог(int repeatCount)
    {
        var (log, _) = CreateLog();
        var toInsert = CreateEntries(1, repeatCount);

        foreach (var entry in toInsert)
        {
            var appended = log.Append(entry);
            log.Commit(appended.Index);
        }

        var actual = log.ReadAllTest();
        Assert.Equal(toInsert, actual, Comparer);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public void AppendRangeИCommit__КогдаЧередуются__ДолжныДобавитьЗаписиВЛог(int repeatCount)
    {
        const int batchSize = 5;
        var (log, _) = CreateLog();
        var entries = CreateEntries(1, repeatCount * batchSize);

        var index = -1;
        foreach (var batch in entries.Chunk(batchSize))
        {
            log.AppendRange(batch);
            index += batchSize;
            log.Commit(index);
        }

        var actual = log.ReadAllTest();
        Assert.Equal(entries, actual, Comparer);
    }


    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(20)]
    public void GetAt__КогдаЛогНеПустойИндексВалидный__ДолженВернутьТребуемоеЗначение(int logSize)
    {
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();

        var (log, _) = CreateLog();
        log.AppendRange(initial);

        for (int index = 0; index < logSize; index++)
        {
            var expected = new LogEntryInfo(initial[index].Term, index);
            var actual = log.GetInfoAt(index);
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public void GetPrecedingEntryInfo__КогдаЛогПустИндекс0__ДолженВернутьTomb()
    {
        var (log, _) = CreateLog();
        var expected = LogEntryInfo.Tomb;

        var actual = log.GetPrecedingLogEntryInfo(0);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    public void GetPrecedingEntryInfo__КогдаЛогНеПустИндекс0__ДолженВернутьTomb(int logSize)
    {
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var expected = LogEntryInfo.Tomb;

        var (log, _) = CreateLog();
        log.AppendRange(initial);

        var actual = log.GetPrecedingLogEntryInfo(0);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(20)]
    public void GetPrecedingEntryInfo__КогдаЛогНеПустИндексРазмерЛога__ДолженВернутьПоследнююЗапись(int logSize)
    {
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var expected = new LogEntryInfo(initial[^1].Term, initial.Length - 1);

        var (log, _) = CreateLog();
        log.AppendRange(initial);

        var actual = log.GetPrecedingLogEntryInfo(logSize);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(20)]
    public void GetPrecedingEntryInfo__КогдаЛогНеПустИндексВДиапазонеЛога__ДолженВернутьКорректнуюЗапись(int logSize)
    {
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var (log, _) = CreateLog();
        log.AppendRange(initial);

        for (int index = 1; index <= logSize; index++)
        {
            var expected = new LogEntryInfo(initial[index - 1].Term, index - 1);

            var actual = log.GetPrecedingLogEntryInfo(index);
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
    public void GetLastLogEntry__СНеПустымЛогом__ДолженВернутьПоследнююЗапись(int logSize)
    {
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();

        var (log, _) = CreateLog();
        log.AppendRange(initial);

        var expected = new LogEntryInfo(initial[^1].Term, initial.Length - 1);
        var actual = log.GetLastLogEntry();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void TruncateUntil__КогдаИндексРавенПоследнемуВЛоге__ДолженОчиститьЛог()
    {
        var (log, _) = CreateLog();
        var entries = new[]
        {
            Entry(1, "asdf"),      // 0
            Entry(2, "data"),      // 1
            Entry(3, "hello"),     // 2
            Entry(4, "  asdf334"), // 3
            Entry(5, "3583w56q4"), // 4
        };
        log.SetupLogTest(entries, Array.Empty<LogEntry>());

        log.TruncateUntil(4);

        log.ReadAllTest()
           .Should()
           .BeEmpty();
    }

    [Theory]
    [InlineData(10, 10, 11, -1)]
    [InlineData(10, 10, 10, -1)]
    [InlineData(10, 10, 9, -1)]
    [InlineData(10, 10, 8, 0)]
    [InlineData(10, 10, 7, 1)]
    [InlineData(10, 10, 0, 8)]
    [InlineData(1, 1, 0, -1)]
    [InlineData(2, 2, 0, 0)]
    [InlineData(2, 2, 1, -1)]
    [InlineData(5, 2, 1, 2)]
    public void TruncateUntil__КогдаЕстьЗакоммиченныеДанные__ДолженКорректноОбновитьИндексКоммита(
        int committedCount,
        int uncommittedCount,
        int removeUntil,
        int expectedCommitIndex)
    {
        var (log, _) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(committedCount + 1, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed, uncommitted);

        log.TruncateUntil(removeUntil);

        Assert.Equal(log.CommitIndex, expectedCommitIndex);
    }

    [Theory]
    [InlineData(5, 5, 0)]
    [InlineData(5, 5, 1)]
    [InlineData(5, 5, 2)]
    [InlineData(5, 5, 4)]
    [InlineData(5, 5, 5)]
    [InlineData(5, 5, 6)]
    [InlineData(5, 5, 9)]
    [InlineData(5, 5, 10)]
    public void TruncateUntil__КогдаВЛогеЕстьЗаписи__ДолженОставитьТолькоЗаписиПослеУказанногоИндекса(
        int committedCount,
        int uncommittedCount,
        int removeUntil)
    {
        var (log, _) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(committedCount + 1, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed, uncommitted);
        var expected = committed.Concat(uncommitted).Skip(removeUntil + 1).ToArray();

        log.TruncateUntil(removeUntil);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void TruncateUntil__КогдаИндексРавенПредпоследнемуВЛоге__ДолженОставитьТолькоПоследнийЭлемент()
    {
        var (log, _) = CreateLog();
        var entries = new[]
        {
            Entry(1, "asdf"),      // 0
            Entry(2, "data"),      // 1
            Entry(3, "hello"),     // 2
            Entry(4, "  asdf334"), // 3
            Entry(5, "3583w56q4"), // 4
        };
        var expected = entries[^1];
        log.SetupLogTest(committed: entries, Array.Empty<LogEntry>());

        log.TruncateUntil(3);

        log.ReadAllTest()
           .Should()
           .ContainSingle("указан предпоследний индекс")
           .Which
           .Should()
           .Be(expected, Comparer, "значение должно быть равно последнему из лога");
    }


    [Fact]
    public void Commit__СНезакоммиченнымиДанными__ДолженЗакоммититьЗапись()
    {
        var (log, _) = CreateLog();
        var entries = new[]
        {
            Entry(1, "dadsfasdf"),       // 0 
            Entry(2, "aaaa"),            // 1
            Entry(3, "uuerhgwerhbwerhw") // 2
        };
        log.AppendRange(entries);
        var commitIndex = 1;

        log.Commit(commitIndex);

        var actualCommitted = log.ReadCommitIndexTest();

        Assert.Equal(commitIndex, actualCommitted);
    }

    [Theory]
    [InlineData(1, 1, 1)]
    [InlineData(1, 2, 1)]
    [InlineData(2, 2, 3)]
    [InlineData(10, 10, 14)]
    [InlineData(12, 123, 50)]
    public void Commit__КогдаЧастьЗакоммиченаИЧастьНет__ДолженОбновитьЗакоммиченнуюЧасть(
        int committedCount,
        int uncommittedCount,
        int commitIndex)
    {
        var (log, _) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(1 + committedCount, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed, uncommitted);
        var (expectedCommitted, expectedUncommitted) = committed.Concat(uncommitted).ToArray().Split(commitIndex);

        log.Commit(commitIndex);

        Assert.Equal(expectedCommitted, log.GetCommittedTest(), Comparer);
        Assert.Equal(expectedUncommitted, log.GetUncommittedTest(), Comparer);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(100, 1)]
    [InlineData(1, 100)]
    public void Commit__КогдаИндексРавенПоследнему__ДолженЗакоммититьВсеЗаписи(int committedCount, int uncommittedCount)
    {
        var (log, _) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(1 + committedCount, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed, uncommitted);
        var expectedCommitted = committed.Concat(uncommitted).ToArray();

        log.Commit(committedCount + uncommittedCount - 1);

        Assert.Equal(expectedCommitted, log.GetCommittedTest(), Comparer);
        Assert.Empty(log.GetUncommittedTest());
    }

    [Theory]
    [InlineData(10, 0)]
    [InlineData(10, 1)]
    [InlineData(10, 9)]
    [InlineData(1, 0)]
    [InlineData(5, 4)]
    public void Commit__КогдаВсеЗаписиНеЗакоммичены__ДолженЗакоммититьПоУказанномуИндексу(
        int uncommittedCount,
        int commitIndex)
    {
        var (log, _) = CreateLog();
        var uncommitted = Enumerable.Range(1, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted);
        var (expectedCommitted, expectedUncommitted) = uncommitted.Split(commitIndex);

        log.Commit(commitIndex);

        Assert.Equal(expectedCommitted, log.GetCommittedTest(), Comparer);
        Assert.Equal(expectedUncommitted, log.GetUncommittedTest(), Comparer);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(0, 1)]
    [InlineData(1, 0)]
    [InlineData(1, 1)]
    [InlineData(5, 10)]
    [InlineData(10, 5)]
    public void AppendRange__НеДолженИзменятьЗначениеКоммита(int committedCount, int uncommittedCount)
    {
        var (log, _) = CreateLog();
        log.SetupLogTest(committed: Enumerable.Range(1, committedCount)
                                              .Select(i => Entry(i, i.ToString()))
                                              .ToArray(),
            uncommitted: Enumerable.Range(committedCount + 2, uncommittedCount)
                                   .Select(i => Entry(i, i.ToString()))
                                   .ToArray());
        var toAppend = new[] {Entry(4, "asdfasdf"), Entry(5, "y5y5trth")};
        var expected = log.CommitIndex;

        log.AppendRange(toAppend);

        Assert.Equal(expected, log.CommitIndex);
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
        log.SetupLogTest(committed: Array.Empty<LogEntry>(),
            uncommitted: initial);
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
        log.SetupLogTest(committed: Array.Empty<LogEntry>(),
            uncommitted: initial);

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
        log.SetupLogTest(committed: Array.Empty<LogEntry>(),
            uncommitted: initial);
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
        log.SetupLogTest(committed: Array.Empty<LogEntry>(),
            uncommitted: initial);
        var toAppend = Enumerable.Range(initialCount + 1, toAppendCount)
                                 .Select(i => Entry(i, i.ToString()))
                                 .ToArray();
        var expected = toAppend;

        log.InsertRangeOverwrite(toAppend, 0);

        Assert.Equal(expected, log.ReadAllTest(), Comparer);
    }

    [Fact]
    public void ReadCommittedData__КогдаЛогПуст__ДолженВернутьПустойМассив()
    {
        var (log, _) = CreateLog();

        var actual = log.ReadCommittedData().ToArray();

        Assert.Empty(actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(100)]
    public void ReadCommittedData__КогдаВсеЗаписиЗакоммичены__ДолженВернутьВсеЗаписи(int committedCount)
    {
        var (log, _) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        log.SetupLogTest(committed, uncommitted: Array.Empty<LogEntry>());
        var expected = committed.Select(e => e.Data).ToArray();

        var actual = log.ReadCommittedData().ToArray();

        Assert.Equal(expected, actual, new LambdaEqualityComparer<byte[]>((l, r) => l.SequenceEqual(r)));
    }

    [Theory]
    [InlineData(10, 10)]
    [InlineData(10, 1)]
    [InlineData(1, 1)]
    [InlineData(5, 2)]
    public void ReadCommittedData__КогдаЕстьЗакоммиченныеИНЕЗакоммиченныеЗаписи__ДолженВернутьТолькоЗакоммиченныеЗаписи(
        int committedCount,
        int uncommittedCount)
    {
        var (log, _) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(committedCount + 1, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed, uncommitted);
        var expected = committed.Select(e => e.Data).ToArray();

        var actual = log.ReadCommittedData().ToArray();

        Assert.Equal(expected, actual, new LambdaEqualityComparer<byte[]>((l, r) => l.SequenceEqual(r)));
    }

    [Fact]
    public void GetUncommitted__КогдаНезакоммиченнаяЗаписьТолько1__ДолженВернутьЭтуЗапись()
    {
        var (log, _) = CreateLog();
        var entry = new LogEntry(new Term(1), "asdfasdf"u8.ToArray());
        var uncommitted = new[] {entry};
        log.SetupLogTest(committed: Array.Empty<LogEntry>(), uncommitted: uncommitted);
        var expected = uncommitted;

        var actual = log.GetUncommittedTest();

        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void GetUncommittedПослеAppend__КогдаЛогБылИзначальноПуст__ДолженВернутьДобавленнуюЗапись()
    {
        var (log, _) = CreateLog();
        var entry = new LogEntry(new Term(1), "asdfasdf"u8.ToArray());
        var expected = new[] {entry};

        log.Append(entry);
        var actual = log.GetUncommittedTest();

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 5)]
    [InlineData(5, 5)]
    [InlineData(10, 10)]
    public void GetUncommitted__КогдаЕстьЗакоммиченныеИНеЗакоммиченные__ДолженВернутьТолькоНезакоммиченныеЗаписи(
        int committedCount,
        int uncommittedCount)
    {
        var (log, _) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(1 + committedCount, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed, uncommitted);
        var expected = uncommitted;

        var actual = log.GetUncommittedTest();

        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void GetUncommitted__КогдаЛогПуст__ДолженВернутьПустойМассив()
    {
        var (log, _) = CreateLog();

        var actual = log.GetUncommittedTest();

        Assert.Empty(actual);
    }

    public void TruncateUntil__КогдаЧастьДанныхБылаУдалена__ДолженОставитьФайлВКорректномСостоянии(
        int committedCount,
        int uncommittedCount,
        int truncateIndex)
    {
        var (log, fs) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(1 + committedCount, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed, uncommitted);
        var expected = committed.Concat(uncommitted).Skip(truncateIndex + 1).ToArray();

        log.TruncateUntil(truncateIndex);
        log.Dispose();


        var operation = ( () => FileLog.Initialize(fs.DataDirectory) );
        operation.Should()
                 .NotThrow("файл должен остаться в корректном состоянии")
                 .Which
                 .ReadAllTest()
                 .Should()
                 .Equal(expected, LogEntryEquals, "данные должны обрезаться");
    }

    [Theory]
    [InlineData(10, 10, 9, 10, 10)]
    [InlineData(10, 10, 19, 5, 0)]
    [InlineData(10, 0, 9, 10, 0)]
    [InlineData(10, 5, 12, 3, 2)]
    public void
        InsertRangeOverwriteПослеTruncateUntil__КогдаДозаписьПроизводитсяВКонец__ДолженКорректноЗаписатьДанныеВКонец(
        int committedCount,
        int uncommittedCount,
        int truncateIndex,
        int insertCount,
        int insertIndex)
    {
        var (log, _) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(1 + committedCount, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed, uncommitted);
        var toInsert = CreateEntries(1 + committedCount + uncommittedCount, insertCount);
        var expected = committed.Concat(uncommitted).Skip(truncateIndex + 1).Concat(toInsert).ToArray();

        log.TruncateUntil(truncateIndex);
        log.InsertRangeOverwrite(toInsert, insertIndex);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(10, 10, 15, 1, 2)]
    [InlineData(10, 10, 14, 5, 3)]
    [InlineData(10, 10, 14, 5, 2)]
    [InlineData(10, 1, 9, 10, 0)]
    [InlineData(10, 5, 12, 3, 1)]
    public void
        InsertRangeOverwriteПослеTruncateUntil__КогдаЧастьЗаписейПерезаписывается__ДолженКорректноПерезаписатьДанные(
        int committedCount,
        int uncommittedCount,
        int truncateIndex,
        int insertCount,
        int insertIndex)
    {
        var (log, _) = CreateLog();
        var committed = Enumerable.Range(1, committedCount)
                                  .Select(i => Entry(i, i.ToString()))
                                  .ToArray();
        var uncommitted = Enumerable.Range(1 + committedCount, uncommittedCount)
                                    .Select(i => Entry(i, i.ToString()))
                                    .ToArray();
        log.SetupLogTest(committed, uncommitted);
        var toInsert = CreateEntries(1 + committedCount + uncommittedCount, insertCount);
        var expected = committed.Concat(uncommitted)
                                .Skip(truncateIndex + 1)
                                .Take(insertIndex)
                                .Concat(toInsert)
                                .ToArray();

        log.TruncateUntil(truncateIndex);
        log.InsertRangeOverwrite(toInsert, insertIndex);

        var actual = log.ReadAllTest();
        Assert.Equal(expected, actual, Comparer);
    }


    private static bool LogEntryEquals(LogEntry left, LogEntry right) =>
        left.Term == right.Term && left.Data.SequenceEqual(right.Data);

    private class LambdaEqualityComparer<T> : IEqualityComparer<T>
    {
        private readonly Func<T, T, bool> _comparer;

        public LambdaEqualityComparer(Func<T, T, bool> comparer)
        {
            _comparer = comparer;
        }

        public bool Equals(T? x, T? y)
        {
            return _comparer(x!, y!);
        }

        public int GetHashCode(T obj)
        {
            return obj!.GetHashCode();
        }
    }
}