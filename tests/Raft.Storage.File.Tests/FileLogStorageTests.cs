using System.ComponentModel;
using Raft.Core;
using Raft.Core.Log;

namespace Raft.Storage.File.Tests;

public class FileLogStorageTests
{
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
        
        var entry = new LogEntry(new Term(1), "some data");
        storage.Append(entry);
        var log = storage.ReadAll();
        
        Assert.Single(log);
    }
    
    [Fact]
    public void ReadLogПослеAppend__КогдаЛогПуст__ДолженВернутьСписокИзТойЖеЗаписи()
    {
        using var memory = new MemoryStream();
        var storage = new FileLogStorage(memory);
        
        var expected = new LogEntry(new Term(1), "some data");
        storage.Append(expected);
        var actual = storage.ReadAll().Single();
        
        Assert.Equal(expected, actual);
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
            var expected = new LogEntry(new Term(i), $"some data {i}");
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
                                 .Select(i => new LogEntry(new Term(i), $"data{i}"))
                                 .ToArray();

        foreach (var entry in expected)
        {
            storage.Append(entry);
        }
        
        var actual = storage.ReadAll();
        Assert.Equal(expected, actual);
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
                                 .Select(i => new LogEntry(new Term(i), $"data{i}"))
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
        var expected = new LogEntry(new Term(2), "sample data");
        
        storage.Append(expected);
        var actual = storage.ReadFrom(0).Single();
        
        Assert.Equal(expected, actual);
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
                                 .Select(i => new LogEntry(new Term(i), $"data{i}"))
                                 .ToArray();

        storage.AppendRange(expected, 0);

        for (int i = 0; i < readCount; i++)
        {
            var actual = storage.ReadAll();
            Assert.Equal(expected, actual);
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
                                .Select(i => new LogEntry(new Term(i), $"data {i}"))
                                .ToArray();

        var firstLog = new FileLogStorage(memory);
        firstLog.AppendRange(entries, 0);
        var secondLog = new FileLogStorage(memory);
        var actual = secondLog.ReadAll();
        
        Assert.Equal(entries, actual);
    }
}