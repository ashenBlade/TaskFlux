using System.Text;
using Raft.Core;
using Raft.Core.Log;

namespace Raft.Storage.InMemory.Tests;

public class InMemoryStorageLogTests
{
    private static LogEntry Entry(int term, string data) => new(new Term(term), Encoding.UTF8.GetBytes(data));
    private static StorageLog CreateLogInMemory(params LogEntry[] initial)
    {
        return new StorageLog(new InMemoryLogStorage(initial));
    }
    
    private static StorageLog CreateLogInMemory(IEnumerable<LogEntry> initial)
    {
        return new StorageLog(new InMemoryLogStorage(initial));
    }


    [Fact]
    public void Contains__КогдаЛогПустойИСравниваемаяПозицияTomb__ДолженВернутьTrue()
    {
        var log = CreateLogInMemory();
        var actual = log.Contains(LogEntryInfo.Tomb);
        Assert.True(actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void GetFrom__СПустымЛогомДолженВернутьПустойМассив(int index)
    {
        var log = CreateLogInMemory();
        var actual = log.GetFrom(index);
        Assert.Empty(actual);
    }

    [Fact]
    public void GetFrom__СЕдинственнымЭлементомЛогаКогдаИндекс0__ДолженВернутьМассивИзЭтогоЭлемента()
    {
        var element = Entry(1, "data");
        var log = CreateLogInMemory(new[] {element});
        var actual = log.GetFrom(0);
        Assert.Single(actual);
        Assert.Equal(actual[0], element);
    }

    [Fact]
    public void GetFrom__СЕдинственнымЭлементомИИндексом1__ДолженВернутьПустойМассив()
    {
        var element = Entry(1, "data");
        var log = CreateLogInMemory(element);
        var actual = log.GetFrom(0);
        Assert.Single(actual);
        Assert.Equal(actual[0], element);
    }
    
    [Theory]
    [InlineData(1, 10)]
    [InlineData(2, 10)]
    [InlineData(3, 10)]
    [InlineData(3, 4)]
    [InlineData(3, 5)]
    [InlineData(5, 5)]
    public void GetFrom__СНесколькимиЭлементами__ДолженВернутьЭлементыСОпределенногоИндекса(int startIndex, int totalCount)
    {
        var first = Enumerable.Range(1, startIndex)
                              .Select(i => Entry(i, "data"))
                              .ToArray();
        var expected = Enumerable.Range(startIndex, totalCount - startIndex)
                                 .Select(i => Entry(i, "data"))
                                 .ToArray();
        
        var log = CreateLogInMemory(first.Concat(expected));

        var actual = log.GetFrom(startIndex);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    public void GetFrom__СИндексомРавнымКоличествуЭлементов__ДолженВернутьПустойМассив(int elementsCount)
    {
        var elements = Enumerable.Range(1, elementsCount)
                                 .Select(i => Entry(i, Random.Shared.Next().ToString()))
                                 .ToArray();
        var log = CreateLogInMemory(elements);
        var actual = log.GetFrom(elementsCount);
        Assert.Empty(actual);
    }

    [Fact]
    public void GetPrecedingEntryInfo__СПустымЛогомИИндексомРавным0__ДолженВернутьTomb()
    {
        var log = CreateLogInMemory();
        var actual = log.GetPrecedingEntryInfo(0);
        Assert.Equal(LogEntryInfo.Tomb, actual);
    }
   
    [Fact]
    public void GetPrecedingEntryInfo__С1ЭлементомВЛогеИИндексомРавным1__ДолженВернутьХранимыйЭлемент()
    {
        var log = CreateLogInMemory(new[] {Entry(1, "data")});
        var expected = new LogEntryInfo(new Term(1), 0);
        
        var actual = log.GetPrecedingEntryInfo(1);
        
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(10)]
    public void GetPrecedingEntryInfo__СНесколькимиЭлементамиВЛоге__ДолженВернутьЭлементСПредыдущимИндексом(int elementsCount)
    {
        var elements = Enumerable.Range(1, elementsCount)
                                 .Select(i => Entry(i, Random.Shared.Next().ToString()))
                                 .ToArray();

        var log = CreateLogInMemory(elements);
        
        for (var i = 1; i < elements.Length; i++)
        {
            var actual = log.GetPrecedingEntryInfo(i);
            Assert.Equal(i - 1, actual.Index);
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public void GetPrecedingEntryInfo__СНесколькимиЭлементамиВЛоге__ДолженВернутьПредыдущийЭлементСТребуемымТермом(
        int elementsCount)
    {
        var elements = Enumerable.Range(1, elementsCount)
                                 .Select(i => Entry(i, Random.Shared.Next().ToString()))
                                 .ToArray();

        var log = CreateLogInMemory(elements);
        
        for (var i = 1; i < elements.Length; i++)
        {
            var actual = log.GetPrecedingEntryInfo(i);
            Assert.Equal(elements[i - 1].Term, actual.Term);
        }
    }

    public static IEnumerable<object[]> InitialAppendedLogEntries = new[]
    {
        new object[]
        {
            new[]
            {
                Entry(1, "data1"), Entry(1, "data2"),
                Entry(1, "data3"), Entry(1, "data4"),
            },
            new[]
            {
                Entry(1, "data5")
            }
        },
        new object[]
        {
            new[]
            {
                Entry(1, "data1"),
            },
            new[]
            {
                Entry(1, "data2"), Entry(2, "data3")
            }
        },
    };

    [Theory]
    [MemberData(nameof(InitialAppendedLogEntries))]
    public void AppendUpdateRange__СИндексомСледующимПослеПоследнегоЭлемента__ДолженДобавитьЭлементыВКонецЛога(LogEntry[] initialEntries, LogEntry[] appended)
    {
        var log = CreateLogInMemory(initialEntries);
        var lastIndex = log.LastEntry.Index;
        log.AppendUpdateRange(appended, lastIndex + 1);
        var actual = log.ReadLog().Skip(lastIndex + 1).ToArray();
        Assert.Equal(appended, actual);
    }

    public static IEnumerable<object[]> LogEntries = new[]
    {
        new object[]
        {
            new[]{Entry(1, "data1")}
        },
        new object[]
        {
            new[]{Entry(1, "data1"), Entry(2, "data2")}
        },
        new object[]
        {
            new[]{Entry(1, "data1"), Entry(2, "data2"), Entry(3, "data3")}
        },
        new object[]
        {
            new[]{Entry(1, "data1"), Entry(2, "data2"), Entry(3, "data3"), Entry(3, "data4"), Entry(3, "data5"), Entry(4, "data6")}
        },
        new object[]
        {
            new[]{Entry(100, "data0"), Entry(190, ""), Entry(2000, "234234")}
        },
        
    };

    [Theory]
    [MemberData(nameof(LogEntries))]
    public void Contains__КогдаЛогНеПустойИПереданПрефиксПустогоЛога__ДолженВернутьTrue(IEnumerable<LogEntry> entries)
    {
        var log = CreateLogInMemory(entries);
        var logEntry = LogEntryInfo.Tomb;
        Assert.True(log.Contains(logEntry));
    }

    [Theory]
    [MemberData(nameof(LogEntries))]
    public void Contains__КогдаЛогНеПустойИПередаетсяПоследнийЭлементЛога__ДолженВернутьTrue(
        IEnumerable<LogEntry> entries)
    {
        var logEntries = entries.ToArray();
        var log = CreateLogInMemory(logEntries);
        var logEntry = logEntries[^1];
        var lastLogEntry = new LogEntryInfo(logEntry.Term, (^1).GetOffset(logEntries.Length));
        Assert.True(log.Contains(lastLogEntry));
    }

    [Theory]
    [MemberData(nameof(LogEntries))]
    public void Contains__КогдаЛогНеПустойИПередаетсяЛюбойЭлементИзЛога__ДолженВернутьTrue(
        IEnumerable<LogEntry> entries)
    {
        var logEntries = entries.ToArray();
        var log = CreateLogInMemory(logEntries);
        
        for (var i = 0; i < logEntries.Length; i++)
        {
            var logEntryInfo = new LogEntryInfo(logEntries[i].Term, i);
            Assert.True(log.Contains(logEntryInfo));
        }
    }
    
    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 2)]
    [InlineData(2, 2)]
    [InlineData(5, 1)]
    [InlineData(2, 10)]
    [InlineData(232, 10)]
    public void Contains__КогдаЛогПустойИПередаетсяНеTomb__ДолженВернутьFalse(
        int term, int index)
    {
        var log = CreateLogInMemory();
        Assert.False(log.Contains(new LogEntryInfo(new Term(term), index)));
    }

    [Theory]
    [MemberData(nameof(LogEntries))]
    public void Contains__КогдаПереданныйИндексБольшеМаксимального__ДолженВернутьFalse(
        IEnumerable<LogEntry> entries)
    {
        var logEntries = entries.ToArray();
        var log = CreateLogInMemory(logEntries);
        var lastLogEntry = new LogEntryInfo(logEntries[^1].Term, logEntries.Length);
        Assert.False(log.Contains(lastLogEntry));
    }

    [Theory]
    [MemberData(nameof(LogEntries))]
    public void Contains__КогдаПереданныйИндексВалидныйНоТермРазличный__ДолженВернутьFalse(
        IEnumerable<LogEntry> entries)
    {
        var logEntries = entries.ToArray();
        var log = CreateLogInMemory(logEntries);
        var index = logEntries.Length / 2;
        var entry = new LogEntryInfo(logEntries[index].Term.Increment(), index);
        Assert.False(log.Contains(entry));
    }

    [Fact]
    public void AppendUpdateRange__КогдаЛогПустИндекс0ПереданныйМассивПуст__НеДолженДобавитьНичегоВЛог()
    {
        var log = CreateLogInMemory();
        log.AppendUpdateRange(Array.Empty<LogEntry>(), 0);
        Assert.Empty(log.ReadLog());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public void AppendUpdateRange__КогдаЛогПустИндекс0ВПереданномМассивеЕстьЭлементы__ДолженДобавитьВсеПереданныеЭлементы(
        int elementsCount)
    {
        var log = CreateLogInMemory();
        var entries = Enumerable.Range(1, elementsCount)
                                .Select(x => Entry(x, $"data{x}"))
                                .ToArray();
        log.AppendUpdateRange(entries, 0);
        Assert.Equal(entries, log.ReadLog());
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 2)]
    [InlineData(2, 1)]
    [InlineData(2, 10)]
    [InlineData(12, 10)]
    public void
        AppendUpdateRange__КогдаВЛогеЕстьЭлементыПереданСледующийПослеПоследнегоЭлементаИндекс__ДолженДобавитьВсеПереданныеЭлементыВКонец(
        int initialElementsCount,
        int toAddCount)
    {
        var initial = Enumerable.Range(1, initialElementsCount)
                                .Select(t => Entry(t, $"data{t}"))
                                .ToArray();
        var toAdd = Enumerable.Range(initialElementsCount + 1, toAddCount)
                              .Select(t => Entry(t, $"data{t}"))
                              .ToArray();
        var log = CreateLogInMemory(initial);
        log.AppendUpdateRange(toAdd, initial.Length);
        Assert.Equal(initial.Concat(toAdd), log.ReadLog());
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 10)]
    [InlineData(10, 2)]
    [InlineData(40, 40)]
    [InlineData(0, 1)]
    [InlineData(0, 0)]
    [InlineData(1, 0)]
    [InlineData(2, 0)]
    public void
        AppendUpdateRange__КогдаВЛогеЕстьЭлементыПереданоНесколькоЭлементовСИндексом0__ДолженЗаменитьТекущийЛогПереданнымиЭлементами(
        int initialCount,
        int toAddCount)
    {
        var initial = Enumerable.Range(1, initialCount)
                                .Select(t => Entry(t, $"data{t}"))
                                .ToArray();
        
        var toAdd = Enumerable.Range(initialCount + 1, toAddCount)
                              .Select(t => Entry(t, $"data{t}"))
                              .ToArray();
        
        var log = CreateLogInMemory(initial);
        log.AppendUpdateRange(toAdd, 0);
        Assert.Equal(toAdd, log.ReadLog());
    }

    [Theory]
    [InlineData(1, 1, 0)]
    [InlineData(5, 2, 3)]
    [InlineData(2, 4, 1)]
    [InlineData(5, 1, 2)]
    public void
        AppendUpdateRange__КогдаВЛогеЕстьЭлементыПереданоНесколькоЭлементовСУказаннымИндексом__ДолженЗаменитьЭлементыСУказанногоИндексаНаПереданныеЭлементы(
        int initialCount,
        int toAddCount,
        int index)
    {
        var initial = Enumerable.Range(1, initialCount)
                                .Select(t => Entry(t, $"data{t}"))
                                .ToArray();
        var toAdd = Enumerable.Range(initialCount + 1, toAddCount)
                              .Select(t => Entry(t, $"data{t}"))
                              .ToArray();
        
        var log = CreateLogInMemory(initial);
        log.AppendUpdateRange(toAdd, index);
        Assert.Equal(initial.Take(index).Concat(toAdd), log.ReadLog());
    }

    public static IEnumerable<object[]> InitialToAddIndexExpected = new[]
    {
        new object[]
        {
            new[]{ Entry(1, "data1"), Entry(1, "data2"), Entry(1, "data3") },
            new[]{Entry(1, "data2"), Entry(2, "data4")},
            1,
            new[] {Entry(1, "data1"), Entry(1, "data2"), Entry(2, "data4")}
        },
        new object[]
        {
            new[]{ Entry(1, "data1"), Entry(2, "data2"), Entry(3, "data3") },
            new[]{ Entry(4, "data2"), Entry(4, "data4") },
            2,
            new[]{ Entry(1, "data1"), Entry(2, "data2"), Entry(4, "data2"), Entry(4, "data4") }
        },
        new object[]
        {
            new[]{ Entry(1, "data1"), Entry(2, "data2"), Entry(3, "data3"), Entry(3, "data5") },
            new[]{ Entry(4, "data2"), Entry(4, "data4") },
            1,
            new[]{ Entry(1, "data1"), Entry(4, "data2"), Entry(4, "data4") }
        },
        new object[]
        {
            new LogEntry[]{ },
            new[]{ Entry(4, "data2"), Entry(4, "data4") },
            0,
            new[]{ Entry(4, "data2"), Entry(4, "data4") }
        },
        new object[]
        {
            new[]{ Entry(4, "data2"), Entry(4, "data4") },
            new LogEntry[]{ },
            0,
            new LogEntry[]{ }
        },
        new object[]
        {
            new[]{ Entry(4, "data2"), Entry(4, "data4") },
            new[]{ Entry(5, "data4"), Entry(6, "data11") },
            1,
            new[]{ Entry(4, "data2"), Entry(5, "data4"), Entry(6, "data11") }
        }
    };

    [Theory]
    [MemberData(nameof(InitialToAddIndexExpected))]
    public void
        AppendUpdateRange__КогдаВПереданномМассивеЕстьСовпадающиеЭлементы__ДолженОбновитьТолькоРазличающиесяЧасти(
        LogEntry[] initial,
        LogEntry[] toAdd,
        int index,
        LogEntry[] expected)
    {
        var log = CreateLogInMemory(initial);
        log.AppendUpdateRange(toAdd, index);
        Assert.Equal(expected, log.ReadLog());
    }

    [Fact]
    public void Conflicts__КогдаЛогПустойИПереданTomb__ДолженВернутьFalse()
    {
        var log = CreateLogInMemory();
        Assert.False(log.Conflicts(LogEntryInfo.Tomb));
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 2)]
    [InlineData(2, 2)]
    [InlineData(2, 6)]
    [InlineData(3, 6)]
    [InlineData(100, 0)]
    [InlineData(100, 10)]
    public void Conflicts__КогдаЛогПустойИПереданВалидныйПрефикс__ДолженВернутьFalse(int term, int index)
    {
        var log = CreateLogInMemory();
        Assert.False(log.Conflicts(new LogEntryInfo(new Term(term), index)));
    }

    
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(10, 5)]
    public void Conflicts__КогдаВЛогеЕстьЭлементыИПереданБольшийТерм__ДолженВернутьFalse(int elementsCount, int index)
    {
        var elements = Enumerable.Range(1, elementsCount)
                                 .Select(t => Entry(t, $"data{t}"))
                                 .ToArray();
        var log = CreateLogInMemory(elements);
        var lastLogEntry = new LogEntryInfo(new(elements.Length + 1), index);
        Assert.False(log.Conflicts(lastLogEntry));
    }

    [Fact]
    public void Conflicts__КогдаПередаетсяСвойЖеПоследнийЭлемент__ДолженВернутьFalse()
    {
        var elements = Enumerable.Range(1, 10)
                                 .Select(t => Entry(t, $"data{t}"))
                                 .ToArray();
        var log = CreateLogInMemory(elements);
        Assert.False(log.Conflicts(log.LastEntry));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public void Conflicts__КогдаПередаетсяЭлементСПоследнимТермомНоБольшимИндексом__ДолженВернутьFalse(int indexDelta)
    {
        var elements = Enumerable.Range(1, 10)
                                 .Select(t => Entry(t, $"data{t}"))
                                 .ToArray();
        var log = CreateLogInMemory(elements);
        var lastEntry = log.LastEntry;
        Assert.False(log.Conflicts(lastEntry with {Index = lastEntry.Index + indexDelta}));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public void Conflicts__КогдаПередаетсяЭлементСОдинаковымПоследнимТермомМеньшимИндексом__ДолженВернутьTrue(
        int indexDelta)
    {
        var elements = Enumerable.Range(1, 10)
                                 .Select(t => Entry(t, $"data{t}"))
                                 .ToArray();
        var log = CreateLogInMemory(elements);
        var lastEntry = log.LastEntry;
        Assert.True(log.Conflicts(lastEntry with {Index = lastEntry.Index - indexDelta}));
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(100, 3)]
    [InlineData(100, 7)]
    public void Conflicts__КогдаПередаетсяЭлементСБольшимИндексомИМеньшимТермом__ДолженВернутьTrue(
        int indexDelta,
        int termDelta)
    {
        var elements = Enumerable.Range(1, 10)
                                 .Select(t => Entry(t, $"data{t}"))
                                 .ToArray();
        var log = CreateLogInMemory(elements);
        var lastEntry = log.LastEntry;
        Assert.True(log.Conflicts(new LogEntryInfo(new( lastEntry.Term.Value - termDelta ), lastEntry.Index + indexDelta)));
    }
}