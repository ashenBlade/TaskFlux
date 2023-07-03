using System.ComponentModel;
using System.Text;
using Raft.Core;
using Raft.Core.Log;

namespace Raft.Storage.File.Tests;

public class FileLogStorageTests
{
    public static LogEntry Entry(int term, string data) 
        => new(new Term(term), Encoding.UTF8.GetBytes(data));
    
    [Fact]
    public void ReadLog__КогдаЛогПуст__ДолженВернутьПустойСписок()
    {
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);
        
        var log = storage.ReadAll();
        
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
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);

        for (int i = 0; i < operationsCount; i++)
        {
            var log = storage.ReadAll();
        
            Assert.Empty(log);
        }
    }
    
    [Fact]
    public void ReadLogПослеAppend__КогдаЛогПуст__ДолженВернутьСписокИзЕдинственнойЗаписи()
    {
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);
        
        var entry = Entry(1, "some data");
        storage.Append(entry);
        var log = storage.ReadAll();
        
        Assert.Single(log);
    }
    
    [Fact]
    public void ReadLogПослеAppend__КогдаЛогПуст__ДолженВернутьСписокИзТойЖеЗаписи()
    {
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);
        
        var expected = Entry(1, "some data");
        storage.Append(expected);
        var actual = storage.ReadAll().Single();
        
        Assert.Equal(expected, actual, LogEntryEqualityComparer.Instance);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    [InlineData(20)]
    public void ReadLogПослеНесколькихAppend__КогдаЛогПуст__ДолженВернутьСписокСТакимЖеКоличествомДобавленныхЗаписей(int entriesCount)
    {
        
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);

        for (int i = 1; i <= entriesCount; i++)
        {
            var expected = Entry(i, $"some data {i}");
            storage.Append(expected);
            
        }
        
        var actual = storage.ReadAll();
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
        
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);

        var expected = Enumerable.Range(1, entriesCount)
                                 .Select(i => Entry(i, $"data{i}"))
                                 .ToArray();

        foreach (var entry in expected)
        {
            storage.Append(entry);
        }
        
        var actual = storage.ReadAll();
        Assert.Equal(expected, actual, LogEntryEqualityComparer.Instance);
    }
    
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    [InlineData(20)]
    public void ReadLogПослеAppendRange__КогдаЛогПуст__ДолженВернутьСписокСТакимЖеКоличествомДобавленныхЗаписей(int entriesCount)
    {
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);

        var expected = Enumerable.Range(1, entriesCount)
                                 .Select(i => Entry(i, $"data{i}"))
                                 .ToArray();

        storage.AppendRange(expected, 0);
        
        var actual = storage.ReadAll();
        Assert.Equal(entriesCount, actual.Count);
    }

    [Fact]
    public void ReadFrom__КогдаЛогПустИИндекс0__ДолженВернутьПустойСписок()
    {
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);

        var actual = storage.ReadFrom(0);
        Assert.Empty(actual);
    }

    [Fact]
    public void ReadFrom__КогдаВЛоге1ЗаписьИИндекс0__ДолженВернутьСписокИзЭтойЗаписи()
    {
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);
        var expected = Entry(2, "sample data");
        
        storage.Append(expected);
        var actual = storage.ReadFrom(0).Single();
        
        Assert.Equal(expected, actual, LogEntryEqualityComparer.Instance);
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
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);

        var expected = Enumerable.Range(1, entriesCount)
                                 .Select(i => Entry(i, $"data{i}"))
                                 .ToArray();

        storage.AppendRange(expected, 0);

        for (int i = 0; i < readCount; i++)
        {
            var actual = storage.ReadAll();
            Assert.Equal(expected, actual, LogEntryEqualityComparer.Instance);
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

        var firstLog = new FileLogStorage(memory);
        firstLog.AppendRange(entries, 0);
        var secondLog = new FileLogStorage(memory);
        var actual = secondLog.ReadAll();
        
        Assert.Equal(entries, actual, LogEntryEqualityComparer.Instance);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 2)]
    [InlineData(2, 1)]
    [InlineData(10, 5)]
    [InlineData(5, 10)]
    public void AppendRange__СНеПустымЛогомИИндексомКонца__ДолженДобавитьЗаписиВКонец(int initialSize, int appendSize)
    {
        using var memory = new MemoryStream();
        var initial = Enumerable.Range(1, initialSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var appended = Enumerable.Range(1 + initialSize, appendSize)
                                 .Select(i => Entry(i, $"data {i}"))
                                 .ToArray();
        var expected = initial.Concat(appended).ToArray();
        
        var firstLog = new FileLogStorage(memory);
        firstLog.AppendRange(initial, 0);
        firstLog.AppendRange(appended, initialSize);
        
        var actual = firstLog.ReadAll();
        Assert.Equal(expected, actual, LogEntryEqualityComparer.Instance);
    }

    [Theory]
    [InlineData(1, 1, 0)]
    [InlineData(1, 2, 0)]
    [InlineData(2, 2, 1)]
    [InlineData(5, 3, 3)]
    [InlineData(5, 5, 2)]
    [InlineData(6, 5, 3)]
    [InlineData(10, 5, 6)]
    [InlineData(10, 5, 7)]
    [InlineData(10, 5, 8)]
    [InlineData(10, 5, 9)]
    public void AppendRange__КогдаЛогНеПустИндексМеньшеМаксимальногоДобавляемыеЗаписиПересекаютПоследнююЗапись__ДолженПерезаписатьЗаписи(
        int initialSize,
        int appendSize,
        int index)
    {
        using var memory = new MemoryStream();
        var initial = Enumerable.Range(1, initialSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var appended = Enumerable.Range(1 + initialSize, appendSize)
                                 .Select(i => Entry(i, $"data {i}"))
                                 .ToArray();
        var expected = initial.Take(index)
                              .Concat(appended)
                              .ToArray();
        
        var firstLog = new FileLogStorage(memory);
        firstLog.AppendRange(initial, 0);
        firstLog.AppendRange(appended, index);
        
        var actual = firstLog.ReadAll();
        Assert.Equal(expected, actual, LogEntryEqualityComparer.Instance);
    }

    [Theory]
    [InlineData(5, 3, 1)]
    [InlineData(10, 3, 0)]
    [InlineData(10, 3, 1)]
    [InlineData(10, 3, 2)]
    [InlineData(10, 3, 3)]
    [InlineData(10, 3, 6)]
    [InlineData(10, 9, 0)]
    [InlineData(10, 8, 1)]
    [InlineData(10, 8, 0)]
    [InlineData(5, 1, 0)]
    [InlineData(5, 1, 2)]
    [InlineData(5, 1, 3)]
    [InlineData(5, 1, 4)]
    public void AppendRange__КогдаЛогНеПустДобавляемыеЗаписиМеньшеПерезаписываемых__ДолженПерезаписатьДанныеИУдалитьСтарые(
        int initialCount,
        int appendCount,
        int index)
    {
        using var memory = new MemoryStream();
        var initial = Enumerable.Range(1, initialCount)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var appended = Enumerable.Range(1 + initialCount, appendCount)
                                 .Select(i => Entry(i, $"data {i}"))
                                 .ToArray();
        
        var expected = initial.Take(index)
                              .Concat(appended)
                              .ToArray();
        
        var firstLog = new FileLogStorage(memory);
        firstLog.AppendRange(initial, 0);
        firstLog.AppendRange(appended, index);
        
        var actual = firstLog.ReadAll();
        Assert.Equal(expected, actual, LogEntryEqualityComparer.Instance);
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
        using var memory = new MemoryStream();
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        
        var log = new FileLogStorage(memory);
        log.AppendRange(initial, 0);
        
        for (int index = 0; index < logSize; index++)
        {
            var expected = new LogEntryInfo( initial[index].Term, index );
            var actual = log.GetAt(index);
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public void GetPrecedingEntryInfo__КогдаЛогПустИндекс0__ДолженВернутьTomb()
    {
        using var memory = new MemoryStream();
        var expected = LogEntryInfo.Tomb;
        
        var log = new FileLogStorage(memory);

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
        using var memory = new MemoryStream();
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var expected = LogEntryInfo.Tomb;
        
        var log = new FileLogStorage(memory);
        log.AppendRange(initial, 0);

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
        using var memory = new MemoryStream();
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        var expected = new LogEntryInfo(initial[^1].Term, initial.Length - 1);
        
        var log = new FileLogStorage(memory);
        log.AppendRange(initial, 0);

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
        using var memory = new MemoryStream();
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        
        var log = new FileLogStorage(memory);
        log.AppendRange(initial, 0);
        
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
        using var memory = new MemoryStream();
        var expected = LogEntryInfo.Tomb;
        
        var log = new FileLogStorage(memory);

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
        using var memory = new MemoryStream();
        var initial = Enumerable.Range(1, logSize)
                                .Select(i => Entry(i, $"data {i}"))
                                .ToArray();
        
        var log = new FileLogStorage(memory);
        log.AppendRange(initial, 0);

        var expected = new LogEntryInfo( initial[^1].Term, initial.Length - 1 );
        var actual = log.GetLastLogEntry();
        Assert.Equal(expected, actual);
    }
}