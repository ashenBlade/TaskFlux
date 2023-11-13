using System.Diagnostics;
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

        var actual = new List<byte[]>();
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
            new object[]
            {
                new (long, byte[])[]
                {
                    ( -6971185620490574902L, new byte[] {37, 9, 248, 53, 100, 186, 245, 208, 213} ),
                    ( -6971185620490574902L, new byte[] {249, 90} ),
                    ( -3044341243339122370L, new byte[] {39, 201, 119, 232, 251, 231, 20} ),
                    ( -3044341243339122370L, new byte[] {132, 6, 237, 231, 211} ),
                    ( 1743799237170239482L, new byte[] {49, 84, 42, 62, 130, 127, 64} ),
                    ( -7907271538582154711L, new byte[] {119, 212, 167, 26, 88} ),
                    ( -3044341243339122370L, new byte[] {64, 61, 238, 99, 248, 41, 68, 11, 253} ),
                    ( 1022184555724037093L, new byte[] { } ),
                    ( -3044341243339122370L, new byte[] {90, 221, 7, 246, 73, 147, 110, 61, 238} ),
                    ( -4115745847243977185L, new byte[] {71, 69} ),
                    ( -6971185620490574902L, new byte[] {235, 102, 192, 166, 139, 199} ),
                    ( -4115745847243977185L, new byte[] {50, 199, 187, 255, 189, 246, 41} ),
                    ( 1022184555724037093L, new byte[] {206, 28, 109, 55, 224, 244, 131, 47, 34} ),
                    ( 917844420766836843L, new byte[] {57, 195, 28, 212, 47, 47, 27, 70} ),
                    ( -4115745847243977185L, new byte[] {95, 248} ),
                    ( 6940236959935815263L, new byte[] {200, 245, 22, 172, 236, 112, 147, 203, 123, 233} ),
                    ( 1022184555724037093L, new byte[] {144, 234, 79, 169, 71} ),
                    ( 2142678183513358935L, new byte[] {63, 86, 42, 182, 250, 53, 42, 54} ),
                    ( -4115745847243977185L, new byte[] { } ),
                    ( 2142678183513358935L, new byte[] {180, 46, 176} ),
                    ( -4115745847243977185L, new byte[] {201, 116, 127, 210} ),
                    ( -6971185620490574902L, new byte[] {251, 185, 166} ),
                    ( 2142678183513358935L, new byte[] {246, 118, 70} ),
                    ( -3044341243339122370L, new byte[] {11, 159} ),
                    ( 2142678183513358935L, new byte[] {100, 219, 36} ),
                    ( -7907271538582154711L, new byte[] {186, 225, 176, 62, 108, 75, 196, 160} ),
                    ( 2142678183513358935L, new byte[] {44, 172} ),
                    ( 2816481699241067466L, new byte[] {250, 232, 184, 210} ),
                    ( -7520943174500530496L, new byte[] {208, 56, 60, 116, 109, 246, 135, 98, 232} ),
                    ( 917844420766836843L, new byte[] {126, 157, 218, 127, 61, 134, 115, 32, 52} ),
                    ( -7520943174500530496L, new byte[] {236, 164, 251, 76} ),
                    ( 695753955246783852L, new byte[] {143, 137, 125, 27, 240, 54, 242, 132, 139, 233} ),
                    ( 1743799237170239482L, new byte[] {55, 192, 47, 155, 44, 196, 124} ),
                    ( -6971185620490574902L, new byte[] {6, 40, 139, 206, 33, 107, 76, 53, 78, 61} ),
                    ( -7907271538582154711L, new byte[] {163, 36, 134} ),
                    ( 695753955246783852L, new byte[] {37, 213, 97, 0, 61, 196} ),
                    ( 1022184555724037093L, new byte[] {43, 111, 180, 150} ),
                    ( -3044341243339122370L, new byte[] {64, 57, 244, 119, 61} ),
                    ( 917844420766836843L, new byte[] {1, 6, 2, 44, 149, 13, 234, 157} ),
                    ( 917844420766836843L, new byte[] {101, 52, 121, 52, 12, 128, 59, 204, 220, 134} ),
                    ( -7907271538582154711L, new byte[] {163, 242, 53, 50, 150} ),
                    ( -7907271538582154711L, new byte[] {161, 213, 101, 250, 188, 71, 136, 150, 68} ),
                    ( 1743799237170239482L, new byte[] {160, 139, 96, 11} ),
                    ( -6971185620490574902L, new byte[] {186, 236, 141, 78} ),
                    ( 4328031195788804646L, new byte[] {134, 59, 132, 168, 62, 11} )
                },
                new (long, byte[])[]
                {
                    ( -7907271538582154711L, new byte[] {119, 212, 167, 26, 88} ),
                    ( -7907271538582154711L, new byte[] {186, 225, 176, 62, 108, 75, 196, 160} ),
                    ( -7907271538582154711L, new byte[] {163, 36, 134} ),
                    ( -7907271538582154711L, new byte[] {163, 242, 53, 50, 150} ),
                    ( -7907271538582154711L, new byte[] {161, 213, 101, 250, 188, 71, 136, 150, 68} ),
                    ( -7520943174500530496L, new byte[] {208, 56, 60, 116, 109, 246, 135, 98, 232} ),
                    ( -7520943174500530496L, new byte[] {236, 164, 251, 76} ),
                    ( -6971185620490574902L, new byte[] {37, 9, 248, 53, 100, 186, 245, 208, 213} ),
                    ( -6971185620490574902L, new byte[] {249, 90} ),
                    ( -6971185620490574902L, new byte[] {235, 102, 192, 166, 139, 199} ),
                    ( -6971185620490574902L, new byte[] {251, 185, 166} ),
                    ( -6971185620490574902L, new byte[] {6, 40, 139, 206, 33, 107, 76, 53, 78, 61} ),
                    ( -6971185620490574902L, new byte[] {186, 236, 141, 78} ),
                    ( -4115745847243977185L, new byte[] {71, 69} ),
                    ( -4115745847243977185L, new byte[] {50, 199, 187, 255, 189, 246, 41} ),
                    ( -4115745847243977185L, new byte[] {95, 248} ), ( -4115745847243977185L, new byte[] { } ),
                    ( -4115745847243977185L, new byte[] {201, 116, 127, 210} ),
                    ( -3044341243339122370L, new byte[] {39, 201, 119, 232, 251, 231, 20} ),
                    ( -3044341243339122370L, new byte[] {132, 6, 237, 231, 211} ),
                    ( -3044341243339122370L, new byte[] {64, 61, 238, 99, 248, 41, 68, 11, 253} ),
                    ( -3044341243339122370L, new byte[] {90, 221, 7, 246, 73, 147, 110, 61, 238} ),
                    ( -3044341243339122370L, new byte[] {11, 159} ),
                    ( -3044341243339122370L, new byte[] {64, 57, 244, 119, 61} ),
                    ( 695753955246783852L, new byte[] {143, 137, 125, 27, 240, 54, 242, 132, 139, 233} ),
                    ( 695753955246783852L, new byte[] {37, 213, 97, 0, 61, 196} ),
                    ( 917844420766836843L, new byte[] {57, 195, 28, 212, 47, 47, 27, 70} ),
                    ( 917844420766836843L, new byte[] {126, 157, 218, 127, 61, 134, 115, 32, 52} ),
                    ( 917844420766836843L, new byte[] {1, 6, 2, 44, 149, 13, 234, 157} ),
                    ( 917844420766836843L, new byte[] {101, 52, 121, 52, 12, 128, 59, 204, 220, 134} ),
                    ( 1022184555724037093L, new byte[] { } ),
                    ( 1022184555724037093L, new byte[] {206, 28, 109, 55, 224, 244, 131, 47, 34} ),
                    ( 1022184555724037093L, new byte[] {144, 234, 79, 169, 71} ),
                    ( 1022184555724037093L, new byte[] {43, 111, 180, 150} ),
                    ( 1743799237170239482L, new byte[] {49, 84, 42, 62, 130, 127, 64} ),
                    ( 1743799237170239482L, new byte[] {55, 192, 47, 155, 44, 196, 124} ),
                    ( 1743799237170239482L, new byte[] {160, 139, 96, 11} ),
                    ( 2142678183513358935L, new byte[] {63, 86, 42, 182, 250, 53, 42, 54} ),
                    ( 2142678183513358935L, new byte[] {180, 46, 176} ),
                    ( 2142678183513358935L, new byte[] {246, 118, 70} ),
                    ( 2142678183513358935L, new byte[] {100, 219, 36} ),
                    ( 2142678183513358935L, new byte[] {44, 172} ),
                    ( 2816481699241067466L, new byte[] {250, 232, 184, 210} ),
                    ( 4328031195788804646L, new byte[] {134, 59, 132, 168, 62, 11} ),
                    ( 6940236959935815263L, new byte[] {200, 245, 22, 172, 236, 112, 147, 203, 123, 233} )
                }
            }
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

        var actual = new List<(long, byte[])>();
        while (queue.TryDequeue(out var k, out var v))
        {
            actual.Add(( k, v ));
        }

        actual
           .Should()
           .Equal(expected, KeyPayloadEqualityComparison);
    }

    [Fact]
    public void TryDequeue__КогдаЕстьМногоЗаписейСРазнымиКлючами__ДолженПолучитьЗаписиПоПриоритету()
    {
        var entries = new (long Key, byte[] Payload)[]
        {
            ( 1L, new byte[] {2, 4, 5, 2, 3} ), ( 0L, new byte[] {2, 45, 2, 55, 34, 90} ),
            ( long.MaxValue, new byte[] {24, 23, 12, 90, 32, 53} ), ( long.MinValue, new byte[] {21, 32, 222, 84} ),
            ( 100L, new byte[] {23, 12, 67, 90, 22, 100} ), ( -200L, new byte[] {2, 1, 23, 87, 4, 100, 254, 255} ),
            ( 123L, new byte[] {4, 1, 45, 124, 55, 1} ), ( -1L, new byte[] {4, 3, 11, 24, 124, 52, 200, 199} ),
            ( -10L, new byte[] {32, 12, 59, 33,} ), ( 10L, new byte[] {2, 2, 2, 2, 2} ),
            ( 90L, new byte[] {24, 2, 100, 101, 102, 103} )
        };
        var expected = entries.OrderBy(x => x.Key).ToArray();
        var queue = new HeapPriorityQueue();

        foreach (var (key, data) in entries)
        {
            queue.Enqueue(key, data);
        }

        var actual = new List<(long, byte[])>();
        while (queue.TryDequeue(out var k, out var v))
        {
            actual.Add(( k, v ));
        }

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
        var (key, data) = ( 1L, new byte[] {23, 2, 11, 80, 200} );
        queue.Enqueue(key, data);

        var actual = queue.ReadAllData();

        actual
           .Should()
           .ContainSingle(tuple => tuple.Priority == key && tuple.Payload.SequenceEqual(data));
    }

    [Fact]
    public void ReadAllData__КогдаВОчередиНесколькоЗаписей__ДолженВернутьВсеЗаписи()
    {
        var queue = new HeapPriorityQueue();
        var data = new (long, byte[])[]
        {
            ( 100L, "hello, world"u8.ToArray() ), ( -1023232412352L, "no way"u8.ToArray() ),
            ( 0L, "xxxxxxxxxxxx"u8.ToArray() ), ( 228L, "qaw4gqwg345634yq3h3qgv"u8.ToArray() ),
            ( 124141251L, "asdf65a4v6s5r1va35rb4*^%&*"u8.ToArray() ), ( -1L, new byte[] {1, 1, 2, 43, 3, 1} ),
            ( 0L, "f124534tg*^&!)(*$__"u8.ToArray() )
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
        queue.Enqueue(1, "hello, world"u8.ToArray());

        var actual = queue.Count;

        actual
           .Should()
           .Be(1, "в очереди 1 элемент");
    }

    [Fact]
    public void Count__КогдаВОчереди2ЭлементаСРазнымиКлючами__ДолженВернуть2()
    {
        var queue = new HeapPriorityQueue();
        queue.Enqueue(1L, "no data"u8.ToArray());
        queue.Enqueue(0L, "another data"u8.ToArray());

        var actual = queue.Count;

        actual
           .Should()
           .Be(2, "в очереди 2 элемента");
    }

    [Fact]
    public void Count__КогдаВОчереди2ЭлементаСОдинаковымиКлючами__ДолженВернуть2()
    {
        var queue = new HeapPriorityQueue();
        queue.Enqueue(1L, "no data"u8.ToArray());
        queue.Enqueue(1L, "another data"u8.ToArray());

        var actual = queue.Count;

        actual
           .Should()
           .Be(2, "в очереди 2 элемента");
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
    public void Count__КогдаВОчередиНесколькоЭлементов__ДолженВернутьЭтоКоличество(int elementsCount)
    {
        var queue = new HeapPriorityQueue();
        foreach (var (key, payload) in Enumerable.Range(0, elementsCount)
                                                 .Select(_ =>
                                                      ( Key: Random.Shared.NextInt64(),
                                                        Payload: "sample data"u8.ToArray() )))
        {
            queue.Enqueue(key, payload);
        }

        var actual = queue.Count;

        actual
           .Should()
           .Be(elementsCount, "было добавлено {0} элеметнов", elementsCount);
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

        var queue = new HeapPriorityQueue();
        foreach (var (key, payload) in Enumerable.Range(0, toEnqueue)
                                                 .Select(_ =>
                                                      ( Key: Random.Shared.NextInt64(), Payload: CreateRandomBytes() )))
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
        var data = Enumerable.Range(0, count)
                             .Select(_ => ( Key: key, Payload: CreateRandomBytes() ))
                             .ToArray();
        foreach (var (k, payload) in data)
        {
            queue.Enqueue(k, payload);
        }

        var result = new List<(long, byte[])>(count);
        while (queue.TryDequeue(out var k, out var payload))
        {
            result.Add(( k, payload ));
        }

        Assert.Equal(result, data, new KeyPayloadComparer());
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

        foreach (var (key, payload) in Enumerable.Range(0, count)
                                                 .Select(_ => ( Key: CreateRandomKey(), Payload: CreateRandomBytes() )))
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

    private class KeyPayloadComparer : IEqualityComparer<(long, byte[])>
    {
        public bool Equals((long, byte[]) x, (long, byte[]) y)
        {
            return x.Item1 == y.Item1 && x.Item2.SequenceEqual(y.Item2);
        }

        public int GetHashCode((long, byte[]) obj)
        {
            return obj.GetHashCode();
        }
    }

    [Fact]
    public void TryDequeue__КогдаВОчереди2ЗаписиСОдинаковымКлючом__ДолженВернутьПервоеДобавленноеЗначение()
    {
        var key = 10L;
        var firstData = "hello, world"u8.ToArray();
        var secondData = "no data lies here"u8.ToArray();
        var queue = new HeapPriorityQueue();

        queue.Enqueue(key, firstData);
        queue.Enqueue(key, secondData);
        queue.TryDequeue(out var actualKey, out var actualData)
             .Should()
             .BeTrue();

        actualKey
           .Should()
           .Be(key, "других ключей нет");

        actualData
           .Should()
           .NotEqual(secondData, "получение должно быть в порядке добавления")
           .And
           .Equal(firstData, "данные не должны быть изменены");
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
        var data = Enumerable.Range(0, count)
                             .Select(_ => ( Key: CreateRandomKey(), Payload: CreateRandomBytes() ))
                             .ToArray();
        var expectedKey = data.MinBy(x => x.Key).Key;
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