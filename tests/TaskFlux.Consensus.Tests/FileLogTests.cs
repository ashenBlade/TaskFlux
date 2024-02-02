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
}