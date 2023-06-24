using Raft.Core;
using Raft.Core.Log;

namespace Raft.Log.InMemory.Tests;

public class InMemoryLogTests
{
    [Fact]
    public void Contains__КогдаЛогПустойИСравниваемаяПозицияTomb__ДолженВернутьTrue()
    {
        var log = new InMemoryLog();
        var actual = log.Contains(LogEntryInfo.Tomb);
        Assert.True(actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void GetFrom__СПустымЛогомДолженВернутьПустойМассив(int index)
    {
        var log = new InMemoryLog();
        var actual = log.GetFrom(index);
        Assert.Empty(actual);
    }

    [Fact]
    public void GetFrom__СЕдинственнымЭлементомЛогаКогдаИндекс0__ДолженВернутьМассивИзЭтогоЭлемента()
    {
        var element = new LogEntry(new Term(1), "data");
        var log = new InMemoryLog(new[] {element});
        var actual = log.GetFrom(0);
        Assert.Single(actual);
        Assert.Equal(actual[0], element);
    }

    [Fact]
    public void GetFrom__СЕдинственнымЭлементомИИндексом1__ДолженВернутьПустойМассив()
    {
        var element = new LogEntry(new Term(1), "data");
        var log = new InMemoryLog(new[] {element});
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
                              .Select(i => new LogEntry(new Term(i), "data"))
                              .ToArray();
        var expected = Enumerable.Range(startIndex, totalCount - startIndex)
                                 .Select(i => new LogEntry(new Term(i), "data"))
                                 .ToArray();
        
        var log = new InMemoryLog(first.Concat(expected));

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
                                 .Select(i => new LogEntry(new Term(i), Random.Shared.Next().ToString()))
                                 .ToArray();
        var log = new InMemoryLog(elements);
        var actual = log.GetFrom(elementsCount);
        Assert.Empty(actual);
    }

    [Fact]
    public void GetPrecedingEntryInfo__СПустымЛогомИИндексомРавным0__ДолженВернутьTomb()
    {
        var log = new InMemoryLog();
        var actual = log.GetPrecedingEntryInfo(0);
        Assert.Equal(LogEntryInfo.Tomb, actual);
    }
   
    [Fact]
    public void GetPrecedingEntryInfo__С1ЭлементомВЛогеИИндексомРавным1__ДолженВернутьХранимыйЭлемент()
    {
        var log = new InMemoryLog(new[] {new LogEntry(new Term(1), "data")});
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
                                 .Select(i => new LogEntry(new Term(i), Random.Shared.Next().ToString()))
                                 .ToArray();

        var log = new InMemoryLog(elements);
        
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
                                 .Select(i => new LogEntry(new Term(i), Random.Shared.Next().ToString()))
                                 .ToArray();

        var log = new InMemoryLog(elements);
        
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
            new LogEntry[]
            {
                new(new Term(1), "data1"), new(new Term(1), "data2"),
                new(new Term(1), "data3"), new(new Term(1), "data4"),
            },
            new LogEntry[]
            {
                new(new (1), "data5")
            }
        },
        new object[]
        {
            new LogEntry[]
            {
                new(new Term(1), "data1"),
            },
            new LogEntry[]
            {
                new(new (1), "data2"), new(new(2), "data3")
            }
        },
    };

    [Theory]
    [MemberData(nameof(InitialAppendedLogEntries))]
    public void AppendUpdateRange__СИндексомСледующимПослеПоследнегоЭлемента__ДолженДобавитьЭлементыВКонецЛога(LogEntry[] initialEntries, LogEntry[] appended)
    {
        var log = new InMemoryLog(initialEntries);
        var lastIndex = log.LastEntry.Index;
        log.AppendUpdateRange(appended, lastIndex + 1);
        var actual = log.Entries.Skip(lastIndex + 1).ToArray();
        Assert.Equal(appended, actual);
    }

    public static IEnumerable<object[]> LogEntries = new[]
    {
        new object[]
        {
            new LogEntry[]{new(new Term(1), "data1")}
        },
        new object[]
        {
            new LogEntry[]{new(new Term(1), "data1"), new(new(2), "data2")}
        },
        new object[]
        {
            new LogEntry[]{new(new(1), "data1"), new(new(2), "data2"), new(new(3), "data3")}
        },
        new object[]
        {
            new LogEntry[]{new(new(1), "data1"), new(new(2), "data2"), new(new(3), "data3"), new(new(3), "data4"), new(new(3), "data5"), new(new(4), "data6")}
        },
        new object[]
        {
            new LogEntry[]{new(new(100), "data0"), new(new(190), ""), new(new(2000), "234234")}
        },
        
    };

    [Theory]
    [MemberData(nameof(LogEntries))]
    public void Contains__КогдаЛогНеПустойИПереданПрефиксПустогоЛога__ДолженВернутьTrue(IEnumerable<LogEntry> entries)
    {
        var log = new InMemoryLog(entries);
        var logEntry = LogEntryInfo.Tomb;
        Assert.True(log.Contains(logEntry));
    }

    [Theory]
    [MemberData(nameof(LogEntries))]
    public void Contains__КогдаЛогНеПустойИПередаетсяПоследнийЭлементЛога__ДолженВернутьTrue(
        IEnumerable<LogEntry> entries)
    {
        var logEntries = entries.ToArray();
        var log = new InMemoryLog(logEntries);
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
        var log = new InMemoryLog(logEntries);
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
        var log = new InMemoryLog();
        Assert.False(log.Contains(new LogEntryInfo(new Term(term), index)));
    }

    [Theory]
    [MemberData(nameof(LogEntries))]
    public void Contains__КогдаПереданныйИндексБольшеМаксимального__ДолженВернутьFalse(
        IEnumerable<LogEntry> entries)
    {
        var logEntries = entries.ToArray();
        var log = new InMemoryLog(logEntries);
        var lastLogEntry = new LogEntryInfo(logEntries[^1].Term, logEntries.Length);
        Assert.False(log.Contains(lastLogEntry));
    }

    [Theory]
    [MemberData(nameof(LogEntries))]
    public void Contains__КогдаПереданныйИндексВалидныйНоТермРазличный__ДолженВернутьFalse(
        IEnumerable<LogEntry> entries)
    {
        var logEntries = entries.ToArray();
        var log = new InMemoryLog(logEntries);
        var index = logEntries.Length / 2;
        var entry = new LogEntryInfo(logEntries[index].Term.Increment(), index);
        Assert.False(log.Contains(entry));
    }
}