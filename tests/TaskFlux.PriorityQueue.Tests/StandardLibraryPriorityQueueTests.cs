using Xunit;

namespace TaskFlux.PriorityQueue.Tests;

[Trait("Category", "BusinessLogic")]
public class StandardLibraryPriorityQueueTests
{
    private static StandardLibraryPriorityQueue<int, int> CreateQueue() => new();

    [Fact]
    public void TryDequeue__СПустойОчередью__ДолженВозвращатьFalse()
    {
        var queue = CreateQueue();
        var success = queue.TryDequeue(out _, out _);
        Assert.False(success);
    }

    [Fact]
    public void TryDequeue__КогдаОчередьНеПуста__ДолженВернутьХранившийсяЭлемент()
    {
        var queue = CreateQueue();
        var expected = ( Key: 1, Value: 2 );
        queue.Enqueue(expected.Key, expected.Value);
        queue.TryDequeue(out var actualKey, out var actualValue);
        var actual = ( actualKey, actualValue );
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void TryDequeue__КогдаОчередьНеПуста__ДолженВернутьTrue()
    {
        var queue = CreateQueue();
        queue.Enqueue(1, 2);
        var success = queue.TryDequeue(out _, out _);
        Assert.True(success);
    }

    [Fact]
    public void Enqueue__СПустойОчередью__ДолженДобавитьЭлементВОчередь()
    {
        var queue = CreateQueue();
        var expected = ( Key: 1, Value: 123 );
        queue.Enqueue(expected.Key, expected.Value);
        queue.TryDequeue(out var key, out var value);
        var actual = ( key, value );
        Assert.Equal(actual, expected);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(20)]
    public void Enqueue__СНепустойОчередью__ДолженДобавитьЭлементВОчередь(int elementsCount)
    {
        var queue = CreateQueue();
        foreach (var i in Enumerable.Range(1, elementsCount))
        {
            queue.Enqueue(i, Random.Shared.Next());
        }

        var expected = ( Key: 1, Value: 123 );
        queue.Enqueue(expected.Key, expected.Value);

        var elements = queue.GetElementsFromQueue();
        Assert.Contains(elements, tuple => tuple == expected);
    }

    [Theory]
    [InlineData(1, 2, 3, 4)]
    [InlineData(-1, 0, 1, 2, 3, 4, 5, 6)]
    [InlineData(int.MinValue, 0, int.MaxValue)]
    [InlineData(6, 5, 4, 3, 2, 1, 0)]
    [InlineData(6, 123, 5, 11, 4, 23, 3, 34, 2, 34, 1, 90, 0)]
    [InlineData(int.MaxValue - 1, int.MaxValue - 2, int.MaxValue - 3, int.MaxValue - 4)]
    [InlineData(int.MaxValue - 10, int.MaxValue - 7, int.MaxValue - 4, int.MaxValue - 1)]
    public void TryDequeue__КогдаВОчередиНесколькоЭлементов__ДолженВернутьЭлементСНаименьшимКлючом(params int[] keys)
    {
        var elements = keys.Select(k => ( Key: k, Value: Random.Shared.Next() )).ToList();
        var expected = elements.MinBy(x => x.Key);

        var queue = CreateQueue();
        foreach (var e in elements)
        {
            queue.Enqueue(e.Key, e.Value);
        }

        queue.TryDequeue(out var actualKey, out var actualValue);
        var actual = ( actualKey, actualValue );

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Count__КогдаОчередьПуста__ДолженВернуть0()
    {
        var queue = CreateQueue();
        Assert.Equal(0, queue.Count);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(100)]
    [InlineData(123)]
    public void Count__КогдаВОчередиЕстьЭлементы__ДолженВернутьИхКоличество(int elementsCount)
    {
        var queue = CreateQueue();

        foreach (var i in Enumerable.Range(0, elementsCount))
        {
            queue.Enqueue(i, Random.Shared.Next());
        }

        Assert.Equal(elementsCount, queue.Count);
    }

    [Fact]
    public void Count__КогдаВОчередьДобавляется2ЭлементаСОдинаковымиКлючами__ДолженДобавитьОбаЭлемента()
    {
        var queue = CreateQueue();
        var first = ( Key: 1, Value: 2 );
        var second = ( Key: 1, Value: 3 );

        queue.Enqueue(first.Key, first.Value);
        queue.Enqueue(second.Key, second.Value);

        Assert.Equal(2, queue.Count);
    }


    [Fact]
    public void
        TryDequeue__КогдаВОчередьДобавляется2ЭлементаСОдинаковымиКлючами__ДолженВернутьПервоеДобавленноеЗначение()
    {
        var queue = CreateQueue();
        var first = ( Key: 1, Value: 2 );
        var second = ( Key: 1, Value: 3 );

        queue.Enqueue(first.Key, first.Value);
        queue.Enqueue(second.Key, second.Value);
        queue.TryDequeue(out var firstKey, out var firstValue);

        var actual = ( firstKey, firstValue );
        Assert.Equal(actual, first);
    }
}