using FluentAssertions;
using TaskFlux.PriorityQueue.Heap;
using Xunit;

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
        var data = CreateRandomBytes();
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
           .Equal(data, "данные не должны быть изменены");
    }

    [Fact]
    public void Enqueue__КогдаОчередьПуста__ДолженДобавитьЭлементВОчередь()
    {
        var data = new byte[] {4, 22, 66, 34, 91, 100};
        var key = 10L;
        var queue = new HeapPriorityQueue();

        queue.Enqueue(key, data);

        var stored = queue.ReadAllDataTest();
        stored
           .Should()
           .Contain(( key, data ), "в хранимых данных должна быть добавленная запись");
    }

    private static long CreateRandomKey() => Random.Shared.NextInt64();

    private static byte[] CreateRandomBytes()
    {
        return Random.Shared.RandomBuffer();
    }

    public static IEnumerable<object[]> TryDequeueКогдаВОчередиНесколькоЭлементВозвращатьВПорядкеПриоритета =>
        new[]
        {
            new object[]
            {
                new (long, byte[])[] {( 10, new byte[] {1, 2, 3, 6,} ), ( 0, new byte[] {9, 92, 12, 35, 2} )},
                ( 0L, new byte[] {9, 92, 12, 35, 2} )
            },
            new object[]
            {
                new (long, byte[])[]
                {
                    ( 10, new byte[] {1, 2, 3, 6,} ), ( 0, new byte[] {9, 92, 12, 35, 2} ),
                    ( 20, new byte[] {90, 89, 8, 77, 22} )
                },
                ( 0L, new byte[] {9, 92, 12, 35, 2} )
            },
            new object[]
            {
                new (long, byte[])[]
                {
                    ( 20, new byte[] {90, 89, 8, 77, 22} ), ( 0, new byte[] {9, 92, 12, 35, 2} ),
                    ( long.MaxValue, new byte[] {9, 92, 12, 35, 2} ), ( -10, new byte[] {1, 2, 3, 6,} ),
                },
                ( -10L, new byte[] {1, 2, 3, 6,} ),
            },
            new object[]
            {
                new (long, byte[])[]
                {
                    ( long.MinValue, new byte[] {90, 89, 8, 77, 22} ), ( 0, new byte[] {9, 92, 12, 35, 2} ),
                    ( long.MaxValue, new byte[] {9, 92, 12, 35, 2} ), ( -10, new byte[] {1, 2, 3, 6,} ),
                },
                ( long.MinValue, new byte[] {90, 89, 8, 77, 22} ),
            }
        };

    [Theory]
    [MemberData(nameof(TryDequeueКогдаВОчередиНесколькоЭлементВозвращатьВПорядкеПриоритета))]
    public void TryDequeue__КогдаВОчередиНесколькоЗаписей__ДолженВозвращатьВПорядкеПриоритета(
        (long, byte[])[] data,
        (long, byte[]) expected)
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
           .Equal(expectedPayload);
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
        var payloads = new byte[][]
        {
            new byte[] {2, 42, 33, 123, 22}, new byte[] {87, 87, 87, 33, 100, 210},
            new byte[] {11, 2, 43, 78, 22, 90}, new byte[] {54, 21, 11, 76, 23}
        };

        var queue = new HeapPriorityQueue();

        foreach (var payload in payloads)
        {
            queue.Enqueue(key, payload);
        }

        var poped = new List<byte[]>();
        while (queue.TryDequeue(out var actualKey, out var actualPayload))
        {
            actualKey
               .Should()
               .Be(key);

            poped.Add(actualPayload);
        }

        poped
           .Should()
           .Equal(payloads, "значения должны быть получены в порядке добавления");
    }

    public static IEnumerable<object[]>
        TryDequeueКогдаЕстьГруппыЗаписейСОдинаковымиКлючамиПоНесколькоЗаписейВКаждойПолучитьВсеЗаписиВПорядкеДобавленияПоПриоритету =>
        new[]
        {
            new object[]
            {
                new (long, byte[])[]
                {
                    ( 1L, new byte[] {2, 43, 44, 123} ), ( -100L, new byte[] {87, 30, 234, 255, 22, 22} ),
                    ( -100L, new byte[] {87, 90, 89, 121} ), ( 1L, new byte[] {2} ),
                    ( 16L, new byte[] {245, 23, 22, 123} ), ( -100L, new byte[] {87, 90, 89, 121, 11} ),
                    ( 0L, new byte[] {2, 1, 0, 0, 0, 0, 0, 0} ), ( -10L, new byte[] {213, 98, 43, 32, 21, 10} )
                },
                new (long, byte[])[]
                {
                    // Отсортировано по возрастанию ключа с сохранением порядка 
                    ( -100L, new byte[] {87, 30, 234, 255, 22, 22} ), ( -100L, new byte[] {87, 90, 89, 121} ),
                    ( -100L, new byte[] {87, 90, 89, 121, 11} ), ( -10L, new byte[] {213, 98, 43, 32, 21, 10} ),
                    ( 0L, new byte[] {2, 1, 0, 0, 0, 0, 0, 0} ), ( 1L, new byte[] {2, 43, 44, 123} ),
                    ( 1L, new byte[] {2} ), ( 16L, new byte[] {245, 23, 22, 123} ),
                }
            },
            new object[]
            {
                new (long, byte[])[]
                {
                    ( 1L, new byte[] {2, 43, 44, 123} ), ( -100L, new byte[] {87, 30, 234, 255, 22, 22} ),
                    ( -100L, new byte[] {87, 90, 89, 121} ), ( 1L, new byte[] {2} ),
                    ( 16L, new byte[] {245, 23, 22, 123} ), ( -100L, new byte[] {87, 90, 89, 121, 11} ),
                    ( 0L, new byte[] {2, 1, 0, 0, 0, 0, 0, 0} ), ( -10L, new byte[] {213, 98, 43, 32, 21, 10} ),
                    ( 1L, new byte[] {2, 43, 44, 123} ), ( -100L, new byte[] {87, 30, 234, 255, 22, 22} ),
                    ( -100L, new byte[] {87, 90, 89, 121} ), ( 1L, new byte[] {2} ),
                    ( 16L, new byte[] {245, 23, 22, 123} ), ( -100L, new byte[] {87, 90, 89, 121, 11} ),
                    ( 0L, new byte[] {2, 1, 0, 0, 0, 0, 0, 0} ), ( -10L, new byte[] {213, 98, 43, 32, 21, 10} ),
                    ( 1L, new byte[] {2, 43, 44, 123} ), ( -100L, new byte[] {87, 30, 234, 255, 22, 22} ),
                    ( -100L, new byte[] {87, 90, 89, 121} ), ( 1L, new byte[] {2} ),
                    ( 16L, new byte[] {245, 23, 22, 123} ), ( -100L, new byte[] {87, 90, 89, 121, 11} ),
                    ( 0L, new byte[] {2, 1, 0, 0, 0, 0, 0, 0} ), ( -10L, new byte[] {213, 98, 43, 32, 21, 10} )
                },
                new (long, byte[])[]
                {
                    // Отсортировано по возрастанию ключа с сохранением порядка 
                    ( -100L, new byte[] {87, 30, 234, 255, 22, 22} ), ( -100L, new byte[] {87, 90, 89, 121} ),
                    ( -100L, new byte[] {87, 90, 89, 121, 11} ), ( -100L, new byte[] {87, 30, 234, 255, 22, 22} ),
                    ( -100L, new byte[] {87, 90, 89, 121} ), ( -100L, new byte[] {87, 90, 89, 121, 11} ),
                    ( -100L, new byte[] {87, 30, 234, 255, 22, 22} ), ( -100L, new byte[] {87, 90, 89, 121} ),
                    ( -100L, new byte[] {87, 90, 89, 121, 11} ), ( -10L, new byte[] {213, 98, 43, 32, 21, 10} ),
                    ( -10L, new byte[] {213, 98, 43, 32, 21, 10} ), ( -10L, new byte[] {213, 98, 43, 32, 21, 10} ),
                    ( 0L, new byte[] {2, 1, 0, 0, 0, 0, 0, 0} ), ( 0L, new byte[] {2, 1, 0, 0, 0, 0, 0, 0} ),
                    ( 0L, new byte[] {2, 1, 0, 0, 0, 0, 0, 0} ), ( 1L, new byte[] {2, 43, 44, 123} ),
                    ( 1L, new byte[] {2} ), ( 1L, new byte[] {2, 43, 44, 123} ), ( 1L, new byte[] {2} ),
                    ( 1L, new byte[] {2, 43, 44, 123} ), ( 1L, new byte[] {2} ), ( 16L, new byte[] {245, 23, 22, 123} ),
                    ( 16L, new byte[] {245, 23, 22, 123} ), ( 16L, new byte[] {245, 23, 22, 123} ),
                }
            },
            new object[]
            {
                new (long, byte[])[]
                {
                    ( -2223072466292555361L, new byte[] { } ), ( -616903668655795556L, new byte[] {137, 109} ),
                    ( 5627181074078724153L, new byte[] {118, 6, 98, 73, 85, 175, 1, 194, 18} ),
                    ( -2223072466292555361L, new byte[] {60, 223, 23} ),
                    ( -2223072466292555361L, new byte[] {4, 44, 117, 10, 47, 71, 27, 105, 104} ),
                    ( -746616892573119318L, new byte[] {215, 174, 155} ),
                    ( -616903668655795556L, new byte[] {214} ),
                    ( -616903668655795556L, new byte[] {199, 50, 132} ),
                    ( -2223072466292555361L, new byte[] {100, 226} ),
                    ( -2223072466292555361L, new byte[] {136, 218, 218, 191, 248, 46, 71, 93, 244} ),
                    ( 5706200618245718511L, new byte[] {107, 12, 176} ),
                    ( -616903668655795556L, new byte[] { } ),
                    ( 5706200618245718511L, new byte[] {164, 128, 113, 236, 200, 127} ),
                    ( 5706200618245718511L, new byte[] {134, 25, 73, 35, 195, 33} ),
                    ( -616903668655795556L, new byte[] {248, 212, 153, 150, 167, 213} ),
                    ( -616903668655795556L, new byte[] {17, 159} ),
                    ( -746616892573119318L, new byte[] {253, 92, 70, 163, 219, 20, 238} ),
                    ( 5706200618245718511L, new byte[] {77, 85} ),
                    ( 8153066530555611705L, new byte[] {163, 88, 97, 72, 211, 68, 243, 209} ),
                    ( 5706200618245718511L, new byte[] { } ),
                    ( -2969979056941715977L, new byte[] {63, 45, 60, 10} ),
                    ( -7467409767805801004L, new byte[] {239, 106, 233, 206, 165} ),
                    ( -2922376339504875608L, new byte[] {239, 96, 125, 253, 150, 74, 132} ),
                    ( -7467409767805801004L, new byte[] {227, 168, 50, 7, 164, 156, 200, 67} ),
                    ( -746616892573119318L, new byte[] {174, 139, 190, 91, 149, 215, 44, 190, 70} ),
                    ( -746616892573119318L, new byte[] {4, 215, 174, 232, 158, 210, 90, 245, 251} ),
                    ( -2969979056941715977L, new byte[] {170, 58, 213, 255, 100, 85, 83, 87} ),
                    ( 5706200618245718511L, new byte[] {196, 7, 42, 234} ),
                    ( 8153066530555611705L, new byte[] {184} ),
                    ( -2922376339504875608L, new byte[] {5, 34, 227, 106} ),
                    ( -7467409767805801004L, new byte[] { } ),
                    ( -2922376339504875608L, new byte[] {250, 12, 184} ),
                    ( -7467409767805801004L, new byte[] {164, 198, 187, 106, 166, 210, 159, 85, 224, 252} ),
                    ( 8153066530555611705L, new byte[] {229, 180, 153, 191} ),
                    ( -2922376339504875608L, new byte[] {41} ),
                    ( -2922376339504875608L, new byte[] {105, 17, 89, 53, 4, 217, 121, 200, 44, 34} ),
                    ( -2922376339504875608L, new byte[] { } )
                },
                new (long, byte[])[]
                {
                    ( -7467409767805801004L, new byte[] {239, 106, 233, 206, 165} ),
                    ( -7467409767805801004L, new byte[] {227, 168, 50, 7, 164, 156, 200, 67} ),
                    ( -7467409767805801004L, new byte[] { } ),
                    ( -7467409767805801004L, new byte[] {164, 198, 187, 106, 166, 210, 159, 85, 224, 252} ),
                    ( -2969979056941715977L, new byte[] {63, 45, 60, 10} ),
                    ( -2969979056941715977L, new byte[] {170, 58, 213, 255, 100, 85, 83, 87} ),
                    ( -2922376339504875608L, new byte[] {239, 96, 125, 253, 150, 74, 132} ),
                    ( -2922376339504875608L, new byte[] {5, 34, 227, 106} ),
                    ( -2922376339504875608L, new byte[] {250, 12, 184} ),
                    ( -2922376339504875608L, new byte[] {41} ),
                    ( -2922376339504875608L, new byte[] {105, 17, 89, 53, 4, 217, 121, 200, 44, 34} ),
                    ( -2922376339504875608L, new byte[] { } ), ( -2223072466292555361L, new byte[] { } ),
                    ( -2223072466292555361L, new byte[] {60, 223, 23} ),
                    ( -2223072466292555361L, new byte[] {4, 44, 117, 10, 47, 71, 27, 105, 104} ),
                    ( -2223072466292555361L, new byte[] {100, 226} ),
                    ( -2223072466292555361L, new byte[] {136, 218, 218, 191, 248, 46, 71, 93, 244} ),
                    ( -746616892573119318L, new byte[] {215, 174, 155} ),
                    ( -746616892573119318L, new byte[] {253, 92, 70, 163, 219, 20, 238} ),
                    ( -746616892573119318L, new byte[] {174, 139, 190, 91, 149, 215, 44, 190, 70} ),
                    ( -746616892573119318L, new byte[] {4, 215, 174, 232, 158, 210, 90, 245, 251} ),
                    ( -616903668655795556L, new byte[] {137, 109} ), ( -616903668655795556L, new byte[] {214} ),
                    ( -616903668655795556L, new byte[] {199, 50, 132} ),
                    ( -616903668655795556L, new byte[] { } ),
                    ( -616903668655795556L, new byte[] {248, 212, 153, 150, 167, 213} ),
                    ( -616903668655795556L, new byte[] {17, 159} ),
                    ( 5627181074078724153L, new byte[] {118, 6, 98, 73, 85, 175, 1, 194, 18} ),
                    ( 5706200618245718511L, new byte[] {107, 12, 176} ),
                    ( 5706200618245718511L, new byte[] {164, 128, 113, 236, 200, 127} ),
                    ( 5706200618245718511L, new byte[] {134, 25, 73, 35, 195, 33} ),
                    ( 5706200618245718511L, new byte[] {77, 85} ), ( 5706200618245718511L, new byte[] { } ),
                    ( 5706200618245718511L, new byte[] {196, 7, 42, 234} ),
                    ( 8153066530555611705L, new byte[] {163, 88, 97, 72, 211, 68, 243, 209} ),
                    ( 8153066530555611705L, new byte[] {184} ),
                    ( 8153066530555611705L, new byte[] {229, 180, 153, 191} )
                }
            },
        };

    [Theory]
    [MemberData(nameof(
        TryDequeueКогдаЕстьГруппыЗаписейСОдинаковымиКлючамиПоНесколькоЗаписейВКаждойПолучитьВсеЗаписиВПорядкеДобавленияПоПриоритету))]
    public void
        TryDequeue__КогдаЕстьГруппыЗаписейСОдинаковымиКлючамиПоНесколькоЗаписейВКаждой__ДолженПолучитьВсеЗаписиВПорядкеДобавленияПоПриоритету(
        (long, byte[])[] data,
        (long, byte[])[] expected)
    {
        var queue = new HeapPriorityQueue();

        foreach (var (key, value) in data)
        {
            queue.Enqueue(key, value);
        }

        var actual = queue.DequeueAllTest();

        actual
           .Should()
           .Equal(expected, KeyPayloadEqualityComparison);
    }

    [Fact]
    public void TryDequeue__КогдаЕстьМногоЗаписейСРазнымиКлючами__ДолженПолучитьЗаписиПоПриоритету()
    {
        var datas = new (long, byte[])[]
        {
            ( 1L, new byte[] {2, 4, 5, 2, 3} ), ( 0L, new byte[] {2, 45, 2, 55, 34, 90} ),
            ( long.MaxValue, new byte[] {24, 23, 12, 90, 32, 53} ), ( long.MinValue, new byte[] {21, 32, 222, 84} ),
            ( 100L, new byte[] {23, 12, 67, 90, 22, 100} ), ( -200L, new byte[] {2, 1, 23, 87, 4, 100, 254, 255} )
        };
        var expected = new (long, byte[])[]
        {
            ( long.MinValue, new byte[] {21, 32, 222, 84} ), ( -200L, new byte[] {2, 1, 23, 87, 4, 100, 254, 255} ),
            ( 0L, new byte[] {2, 45, 2, 55, 34, 90} ), ( 1L, new byte[] {2, 4, 5, 2, 3} ),
            ( 100L, new byte[] {23, 12, 67, 90, 22, 100} ), ( long.MaxValue, new byte[] {24, 23, 12, 90, 32, 53} ),
        };
        var queue = new HeapPriorityQueue();

        foreach (var (key, data) in datas)
        {
            queue.Enqueue(key, data);
        }

        var actual = queue.DequeueAllTest();

        actual
           .Should()
           .Equal(expected, KeyPayloadEqualityComparison);
    }


    private static bool KeyPayloadEqualityComparison((long Key, byte[] Payload) actualTuple,
                                                     (long Key, byte[] Payload) expectedTuple)
    {
        return actualTuple.Key == expectedTuple.Key
            && actualTuple.Payload.SequenceEqual(expectedTuple.Payload);
    }
}