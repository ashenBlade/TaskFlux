using System.Diagnostics;
using FluentAssertions;
using Xunit;
using HeapPriorityQueue = TaskFlux.PriorityQueue.Heap.HeapPriorityQueue<int>;

namespace TaskFlux.PriorityQueue.Tests;

[Trait("Category", "BusinessLogic")]
public class HeapPriorityQueueTests
{
    [Fact]
    public void Count__КогдаОчередьПуста__ДолженВернуть0()
    {
        var queue = new HeapPriorityQueue();

        var actual = queue.Count;

        actual
           .Should()
           .Be(0, "очередь только была создана");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1L)]
    [InlineData(1L)]
    [InlineData(123L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void TryDequeue__КогдаВОчереди1Элемент__ДолженВернутьХранившийсяЭлемент(long key)
    {
        var data = 123;
        var queue = new HeapPriorityQueue();
        queue.Enqueue(key, data);

        var success = queue.TryDequeue(out var actualKey, out var actualValue);
        success
           .Should()
           .BeTrue("очередь была не пуста");

        actualKey
           .Should()
           .Be(key, "добавленный ключ должен быть равен полученному");
        actualValue
           .Should()
           .Be(data, "данные не должны быть изменены");
    }

    [Fact]
    public void Enqueue__КогдаОчередьПуста__ДолженДобавитьЭлементВОчередь()
    {
        var data = 5555;
        var key = 10L;
        var queue = new HeapPriorityQueue();

        queue.Enqueue(key, data);

        var stored = queue.ReadAllDataTest();
        stored
           .Should()
           .Contain(( key, data ), "в хранимых данных должна быть добавленная запись");
    }

    private static long CreateRandomPriority() => Random.Shared.NextInt64();

    public static IEnumerable<object[]> TryDequeueКогдаВОчередиНесколькоЭлементВозвращатьВПорядкеПриоритета =>
        new object[][]
        {
            [
                new (long, int)[] {( 10, 123 ), ( 0, 444 )},
                ( 0L, 444 )
            ],
            [
                new (long, int)[] {( 10, 7865 ), ( 0, 345262 ), ( 20, -435888 )},
                ( 0L, 345262 )
            ],
            [
                new (long, int)[] {( 20, 444 ), ( 0, -1 ), ( long.MaxValue, 6545646 ), ( -10, 48358 ),},
                ( -10L, 48358 )
            ],
            [
                new (long, int)[] {( long.MinValue, 123 ), ( 0, 9999 ), ( long.MaxValue, 455 ), ( -10, 228 ),},
                ( long.MinValue, 123 )
            ]
        };

    [Theory]
    [MemberData(nameof(TryDequeueКогдаВОчередиНесколькоЭлементВозвращатьВПорядкеПриоритета))]
    public void TryDequeue__КогдаВОчередиНесколькоЗаписей__ДолженВозвращатьВПорядкеПриоритета(
        (long, int)[] data,
        (long, int) expected)
    {
        var (expectedKey, expectedPayload) = expected;
        var queue = new HeapPriorityQueue();
        foreach (var (key, payload) in data)
        {
            queue.Enqueue(key, payload);
        }

        var (actualKey, actualPayload) = queue.Dequeue();

        actualKey
           .Should()
           .Be(expectedKey);

        actualPayload
           .Should()
           .Be(expectedPayload);
    }

    [Fact]
    public void TryDequeue__КогдаОчередьПуста__ДолженВернутьFalse()
    {
        var queue = new HeapPriorityQueue();

        var actual = queue.TryDequeue(out _, out _);

        actual.Should().BeFalse("очередь пуста");
    }

    [Fact]
    public void TryDequeue__КогдаВОчередиНесколькоЗаписейСОдинаковымКлючом__ДолженВозвращатьЭлементыВПорядкеДобавления()
    {
        var key = 10L;
        var payloads = new[] {34343, 123123, -1111, 0};
        var queue = new HeapPriorityQueue();

        foreach (var payload in payloads)
        {
            queue.Enqueue(key, payload);
        }

        var actual = new List<int>();
        while (queue.TryDequeue(out var actualKey, out var actualPayload))
        {
            actualKey
               .Should()
               .Be(key);

            actual.Add(actualPayload);
        }

        actual
           .Should()
           .Equal(payloads, "значения должны быть получены в порядке добавления");
    }

    public static IEnumerable<object[]>
        TryDequeueКогдаЕстьГруппыЗаписейСОдинаковымиКлючамиПоНесколькоЗаписейВКаждойПолучитьВсеЗаписиВПорядкеДобавленияПоПриоритету =>
        new[]
        {
            new object[]
            {
                new (long, int)[]
                {
                    ( 1L, 123 ), ( -100L, 3333 ), ( -100L, 7245 ), ( 1L, 56856 ), ( 16L, 1424 ),
                    ( -100L, 7652 ), ( 0L, 1346123 ), ( -10L, int.MinValue )
                }
            },
            new object[]
            {
                new (long, int)[]
                {
                    ( 1L, 1346 ), ( -100L, 8765432 ), ( -100L, 87 ), ( 1L, 2 ), ( 16L, 245 ), ( -100L, 87 ),
                    ( 0L, 2 ), ( -10L, 213 ), ( 1L, 2 ), ( -100L, 87 ), ( -100L, 87 ), ( 1L, 2 ),
                    ( 16L, 245 ), ( -100L, 87 ), ( 0L, 2 ), ( -10L, 213 ), ( 1L, 2 ), ( -100L, 87 ),
                    ( -100L, 87 ), ( 1L, 2 ), ( 16L, 245 ), ( -100L, 87 ), ( 0L, 2 ), ( -10L, 213 )
                }
            },
            new object[]
            {
                new (long, int)[]
                {
                    ( -2223072466292555361L, 876541 ), ( -616903668655795556L, 137 ),
                    ( 5627181074078724153L, 118 ), ( -2223072466292555361L, 60 ),
                    ( -2223072466292555361L, 4 ), ( -746616892573119318L, 215 ),
                    ( -616903668655795556L, 214 ), ( -616903668655795556L, 199 ),
                    ( -2223072466292555361L, 100 ), ( -2223072466292555361L, 136 ),
                    ( 5706200618245718511L, 107 ), ( -616903668655795556L, 145235 ),
                    ( 5706200618245718511L, 164 ), ( 5706200618245718511L, 134 ),
                    ( -616903668655795556L, 248 ), ( -616903668655795556L, 17 ),
                    ( -746616892573119318L, 253 ), ( 5706200618245718511L, 77 ),
                    ( 8153066530555611705L, 163 ), ( 5706200618245718511L, 0 ),
                    ( -2969979056941715977L, 63 ), ( -7467409767805801004L, 239 ),
                    ( -2922376339504875608L, 239 ), ( -7467409767805801004L, 227 ),
                    ( -746616892573119318L, 174 ), ( -746616892573119318L, 4 ),
                    ( -2969979056941715977L, 170 ), ( 5706200618245718511L, 196 ),
                    ( 8153066530555611705L, 184 ), ( -2922376339504875608L, 5 ),
                    ( -7467409767805801004L, 0 ), ( -2922376339504875608L, 250 ),
                    ( -7467409767805801004L, 164 ), ( 8153066530555611705L, 229 ),
                    ( -2922376339504875608L, 41 ), ( -2922376339504875608L, 105 ),
                    ( -2922376339504875608L, 0 )
                }
            },
            new object[]
            {
                new (long, int)[]
                {
                    ( -6971185620490574902L, 37 ), ( -6971185620490574902L, 249 ),
                    ( -3044341243339122370L, 39 ), ( -3044341243339122370L, 132 ),
                    ( 1743799237170239482L, 49 ), ( -7907271538582154711L, 119 ),
                    ( -3044341243339122370L, 64 ), ( 1022184555724037093L, 0 ),
                    ( -3044341243339122370L, 90 ), ( -4115745847243977185L, 71 ),
                    ( -6971185620490574902L, 235 ), ( -4115745847243977185L, 50 ),
                    ( 1022184555724037093L, 206 ), ( 917844420766836843L, 57 ),
                    ( -4115745847243977185L, 95 ), ( 6940236959935815263L, 200 ),
                    ( 1022184555724037093L, 144 ), ( 2142678183513358935L, 63 ),
                    ( -4115745847243977185L, 0 ), ( 2142678183513358935L, 180 ),
                    ( -4115745847243977185L, 201 ), ( -6971185620490574902L, 251 ),
                    ( 2142678183513358935L, 246 ), ( -3044341243339122370L, 11 ),
                    ( 2142678183513358935L, 100 ), ( -7907271538582154711L, 186 ),
                    ( 2142678183513358935L, 44 ), ( 2816481699241067466L, 250 ),
                    ( -7520943174500530496L, 208 ), ( 917844420766836843L, 126 ),
                    ( -7520943174500530496L, 236 ), ( 695753955246783852L, 143 ),
                    ( 1743799237170239482L, 55 ), ( -6971185620490574902L, 6 ),
                    ( -7907271538582154711L, 163 ), ( 695753955246783852L, 37 ),
                    ( 1022184555724037093L, 43 ), ( -3044341243339122370L, 64 ), ( 917844420766836843L, 1 ),
                    ( 917844420766836843L, 101 ), ( -7907271538582154711L, 163 ),
                    ( -7907271538582154711L, 161 ), ( 1743799237170239482L, 160 ),
                    ( -6971185620490574902L, 186 ), ( 4328031195788804646L, 134 )
                }
            }
        };

    [Theory]
    [MemberData(nameof(
        TryDequeueКогдаЕстьГруппыЗаписейСОдинаковымиКлючамиПоНесколькоЗаписейВКаждойПолучитьВсеЗаписиВПорядкеДобавленияПоПриоритету))]
    public void
        TryDequeue__КогдаЕстьГруппыЗаписейСОдинаковымиКлючамиПоНесколькоЗаписейВКаждой__ДолженПолучитьВсеЗаписиВПорядкеДобавленияПоПриоритету(
        (long Priority, int)[] data)
    {
        // OrderBy - стабильная сортировка
        var expected = data.OrderBy(x => x.Priority).ToArray();
        var queue = new HeapPriorityQueue();

        foreach (var (key, value) in data)
        {
            queue.Enqueue(key, value);
        }

        var actual = new List<(long, int)>();
        while (queue.TryDequeue(out var k, out var v))
        {
            actual.Add(( k, v ));
        }

        actual
           .Should()
           .Equal(expected);
    }

    [Fact]
    public void TryDequeue__КогдаЕстьМногоЗаписейСРазнымиКлючами__ДолженПолучитьЗаписиПоПриоритету()
    {
        var entries = new (long Key, int Payload)[]
        {
            ( 1L, 2 ), ( 0L, 2 ), ( long.MaxValue, 24 ), ( long.MinValue, 21 ), ( 100L, 23 ), ( -200L, 2 ),
            ( 123L, 4 ), ( -1L, 4 ), ( -10L, 32 ), ( 10L, 2 ), ( 90L, 24 )
        };
        var expected = entries.OrderBy(x => x.Key).ToArray();

        var queue = new HeapPriorityQueue();

        foreach (var (key, data) in entries)
        {
            queue.Enqueue(key, data);
        }

        var actual = new List<(long, int)>();
        while (queue.TryDequeue(out var k, out var v))
        {
            actual.Add(( k, v ));
        }

        actual
           .Should()
           .Equal(expected);
    }


    [Fact]
    public void ReadAllData__КогдаОчередьПуста__ДолженВернутьПустойМассив()
    {
        var queue = new HeapPriorityQueue();

        var stored = queue.ReadAllData();

        stored
           .Should()
           .BeEmpty("в очереди нет записей");
    }

    [Fact]
    public void RealAllData__КогдаВОчередиЕстьЗапись__ДолженВернутьЭтуЗапись()
    {
        var queue = new HeapPriorityQueue();
        var (priority, data) = ( 1L, 23 );
        queue.Enqueue(priority, data);

        var actual = queue.ReadAllData();

        actual
           .Should()
           .ContainSingle(tuple => tuple.Priority == priority && tuple.Data == data);
    }

    [Fact]
    public void ReadAllData__КогдаВОчередиНесколькоЗаписей__ДолженВернутьВсеЗаписи()
    {
        var queue = new HeapPriorityQueue();
        var data = new (long, int)[]
        {
            ( 100L, 163416436 ), ( -1023232412352L, int.MaxValue ), ( 0L, int.MaxValue ), ( 228L, int.MaxValue ),
            ( 124141251L, int.MaxValue ), ( -1L, 1 ), ( 0L, int.MinValue )
        };
        foreach (var (key, payload) in data)
        {
            queue.Enqueue(key, payload);
        }

        var actual = queue.ReadAllData();

        actual
           .Should()
           .Contain(data);
    }

    [Fact]
    public void Count__КогдаВОчередиОдинЭлемент__ДолженВернуть1()
    {
        var queue = new HeapPriorityQueue();
        queue.Enqueue(1, 124124);

        var actual = queue.Count;

        actual
           .Should()
           .Be(1, "в очереди 1 элемент");
    }

    [Fact]
    public void Count__КогдаВОчереди2ЭлементаСРазнымиКлючами__ДолженВернуть2()
    {
        var queue = new HeapPriorityQueue();
        queue.Enqueue(1L, 124321);
        queue.Enqueue(0L, 2354234);

        var actual = queue.Count;

        actual
           .Should()
           .Be(2, "в очереди 2 элемента");
    }

    [Fact]
    public void Count__КогдаВОчереди2ЭлементаСОдинаковымиКлючами__ДолженВернуть2()
    {
        var queue = new HeapPriorityQueue();
        queue.Enqueue(1L, 135765);
        queue.Enqueue(1L, 3465345);

        var actual = queue.Count;

        actual
           .Should()
           .Be(2, "в очереди 2 элемента");
    }

    [Fact]
    public void Count__КогдаВОчереди2ЭлементаСОдинаковымиКлючамиИСодержимым__ДолженВернуть2()
    {
        var queue = new HeapPriorityQueue();
        queue.Enqueue(1L, 135765);
        queue.Enqueue(1L, 135765);

        var actual = queue.Count;

        actual
           .Should()
           .Be(2, "в очередь добавлено 2 элемента");
    }

    [Theory]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(10)]
    [InlineData(256)]
    [InlineData(257)]
    [InlineData(512)]
    [InlineData(1000)]
    [InlineData(2000)]
    public void Count__КогдаВОчередиНесколькоЭлементов__ДолженВернутьДобавленноеКоличествоЭлементов(int elementsCount)
    {
        var queue = new HeapPriorityQueue();
        var random = new Random(15);
        foreach (var (key, payload) in Enumerable.Range(0, elementsCount)
                                                 .Select(_ => ( Priority: random.NextInt64(), Payload: random.Next() )))
        {
            queue.Enqueue(key, payload);
        }

        var actual = queue.Count;

        actual
           .Should()
           .Be(elementsCount, "было добавлено {0} элементов", elementsCount);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(10, 1)]
    [InlineData(16, 12)]
    [InlineData(100, 1)]
    [InlineData(100, 50)]
    [InlineData(1000, 100)]
    [InlineData(10000, 7234)]
    public void Count__КогдаИзОчередиБылиУдаленыЭлементы__ДолженОбновитьCount(int toEnqueue, int toDequeue)
    {
        var expected = toEnqueue - toDequeue;
        Debug.Assert(expected >= 0);

        var random = new Random(7654);
        var queue = new HeapPriorityQueue();
        foreach (var (key, payload) in Enumerable.Range(0, toEnqueue)
                                                 .Select(_ =>
                                                      ( Priority: random.NextInt64(), Data: random.Next() )))
        {
            queue.Enqueue(key, payload);
        }

        for (int i = 0; i < toDequeue; i++)
        {
            queue.TryDequeue(out _, out _);
        }

        var actual = queue.Count;

        actual
           .Should()
           .Be(expected, "после удаления записей размер должен обновиться");
    }

    [Theory]
    [InlineData(10)]
    [InlineData(128)]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(257)]
    [InlineData(511)]
    [InlineData(512)]
    [InlineData(513)]
    [InlineData(514)]
    [InlineData(1000)]
    [InlineData(2000)]
    public void TryDequeue__КогдаМногоЭлементовСОдинаковымКлючом__ДолженПолучитьВсеЭлементы(int count)
    {
        var key = 10L;
        var queue = new HeapPriorityQueue();
        var random = new Random(36436);
        var data = Enumerable.Range(0, count)
                             .Select(_ => ( Key: key, Payload: random.Next() ))
                             .ToArray();
        foreach (var (k, payload) in data)
        {
            queue.Enqueue(k, payload);
        }

        var result = new List<(long, int)>(count);
        while (queue.TryDequeue(out var k, out var payload))
        {
            result.Add(( k, payload ));
        }

        Assert.Equal(result, data);
    }

    [Theory]
    [InlineData(10)]
    [InlineData(50)]
    [InlineData(100)]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(257)]
    [InlineData(1000)]
    [InlineData(2000)]
    [InlineData(3000)]
    public void TryDequeue__КогдаОчередьСталаПуста__ДолженВернутьFalse(int count)
    {
        var queue = new HeapPriorityQueue();

        var random = new Random(7777);
        foreach (var (key, payload) in Enumerable.Range(0, count)
                                                 .Select(_ => ( Priority: random.NextInt64(), Payload: random.Next() )))
        {
            queue.Enqueue(key, payload);
        }

        var enqueuedCount = 0;
        while (queue.TryDequeue(out _, out _))
        {
            enqueuedCount++;
        }

        var actual = queue.TryDequeue(out _, out _);

        enqueuedCount
           .Should()
           .Be(count, "количество полученных записей должно быть равно записанному");
        actual
           .Should()
           .BeFalse("очередь пуста - из нее взяли все элементы");
    }

    [Fact]
    public void TryDequeue__КогдаВОчереди2ЗаписиСОдинаковымКлючом__ДолженВернутьПервоеДобавленноеЗначение()
    {
        var key = 10L;
        var firstData = 87666;
        var secondData = 22222;
        var queue = new HeapPriorityQueue();

        queue.Enqueue(key, firstData);
        queue.Enqueue(key, secondData);
        queue.TryDequeue(out var actualKey, out var actualData)
             .Should()
             .BeTrue();

        actualKey
           .Should()
           .Be(key, "других приоритетов нет");

        actualData
           .Should()
           .NotBe(secondData, "чтение должно быть в порядке добавления")
           .And
           .Be(firstData, "данные не должны быть изменены");
    }

    [Theory]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(50)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(2000)]
    [InlineData(5000)]
    public void TryDequeue__КогдаВОчередиНесколькоЭлементов__ДолженВернутьСНаименьшимКлючом(int count)
    {
        var random = new Random(777);
        var data = Enumerable.Range(0, count)
                             .Select(_ => ( Priority: random.NextInt64(), Payload: random.Next() ))
                             .ToArray();
        var expectedKey = data.MinBy(x => x.Priority).Priority;
        var queue = new HeapPriorityQueue();
        foreach (var (key, payload) in data)
        {
            queue.Enqueue(key, payload);
        }

        queue.TryDequeue(out var actualKey, out _)
             .Should()
             .BeTrue();

        actualKey
           .Should()
           .Be(expectedKey, "должен быть получен ключ с наименьшим значением");
    }
}