using FluentAssertions;
using TaskFlux.PriorityQueue.ArrayList;
using Xunit;

namespace TaskFlux.PriorityQueue.Tests;

public class ArrayListPriorityQueueTests
{
    private static ArrayListPriorityQueue<byte[]> CreateQueue(long min,
                                                              long max,
                                                              IEnumerable<(long, byte[])>? payload = null) =>
        payload is null
            ? new(min, max)
            : new(min, max, payload);

    private static readonly byte[] StubPayload = {1, 2, 3, 10, 144};

    [Fact]
    public void Enqueue__КогдаКлюч0_ДиапазонМни10До10__ДолженВставитьЭлемент()
    {
        var queue = CreateQueue(-10, 10);
        var payload = new byte[] {1, 2, 4,};
        var key = 0L;

        queue.Enqueue(key, payload);

        var allData = queue.ToListUnordered();
        allData
           .Should()
           .ContainSingle(x => x.Item1 == key && x.Item2.SequenceEqual(payload),
                "В очередь добавлена единственная запись");
    }

    [Fact]
    public void Enqueue__КогдаКлючРавенГраничному__ДолженВставитьЭлемент()
    {
        var minKey = -10L;
        var maxKey = 10L;

        // Минимальный ключ
        {
            var queue = CreateQueue(minKey, maxKey);
            var payload = StubPayload;

            queue.Enqueue(minKey, payload);

            var allData = queue.ToListUnordered();

            allData
               .Should()
               .ContainSingle(x => x.Item1 == minKey && x.Item2.SequenceEqual(payload),
                    "Минимальный ключ должен включаться");
        }

        // Максимальный ключ
        {
            var queue = CreateQueue(minKey, maxKey);
            var payload = StubPayload;

            queue.Enqueue(maxKey, payload);

            var allData = queue.ToListUnordered();
            allData
               .Should()
               .ContainSingle(x => x.Item1 == maxKey && x.Item2.SequenceEqual(payload),
                    "Максимальный ключ должен включаться");
        }
    }

    [Fact]
    public void Enqueue__КогдаКлючВыходитЗаПределыДиапазона__ДолженКинутьИсключение()
    {
        var minKey = -25L;
        var maxKey = 100L;
        var key = 101L;
        var queue = CreateQueue(minKey, maxKey);

        // Пока не определился нужно ли задавать конкретное исключение, 
        // поэтому сейчас будем кидать любое исключение 
        Assert.ThrowsAny<InvalidKeyRangeException>(() => queue.Enqueue(key, StubPayload));
    }

    [Fact]
    public void TryDequeue__КогдаОчередьПуста__ДолженВернутьFalse()
    {
        var queue = CreateQueue(-1, 1);

        var success = queue.TryDequeue(out _, out _);

        success
           .Should()
           .BeFalse("Очередь пуста - нечего брать");
    }

    [Fact]
    public void TryDequeue__КогдаВОчереди1Элемент__ДолженВернутьЭтотЭлемент()
    {
        var key = 0L;
        var payload = new byte[] {1, 56, 11, 80};

        var record = ( key, payload );
        var queue = CreateQueue(-1, 1, new[] {record});

        var success = queue.TryDequeue(out var actualKey, out var actualPayload);

        success
           .Should()
           .BeTrue("Очередь не была пуста");

        actualKey
           .Should()
           .Be(key, "Вставленный и полученный ключи должны быть одинаковы");

        actualPayload
           .Should()
           .Equal(payload, "Хранящиеся данные не должны быть измненены");
    }

    [Fact]
    public void Count__КогдаОчередьТолькоСоздана__ДолженВернуть0()
    {
        var queue = CreateQueue(-1, 1);

        var count = queue.Count;

        count
           .Should()
           .Be(0, "При создании очередь пуста");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    public void Count__КогдаВПустуюОчередьДобавилиНесколькоЭлементов__ДолженВернутьДобавленноеКоличествоЭлементов(
        int elementsCount)
    {
        var min = -10L;
        var max = 10L;
        var queue = CreateQueue(min, max);
        var elements = Enumerable.Range(0, elementsCount)
                                 .Select(_ => ( Key: Random.Shared.NextInt64(min, max + 1), Data: RandomData() ))
                                 .ToArray();

        foreach (var (key, data) in elements)
        {
            queue.Enqueue(key, data);
        }

        var actualCount = queue.Count;

        actualCount
           .Should()
           .Be(elementsCount,
                "размер очереди должен быть равен количеству добавленных записей, так как ничего не забирали");
    }

    [Fact]
    public void ЧтениеСОдинмИТемЖеКлючом__ДолжноВозвращатьЭлементыВПорядкеДобавления()
    {
        var key = 1L;
        var elements = new (long, byte[])[]
        {
            ( key, new byte[] {1} ), ( key, new byte[] {2} ), ( key, new byte[] {3} ), ( key, new byte[] {4} ),
            ( key, new byte[] {5} ),
        };
        var queue = CreateQueue(-10L, 10L);

        // Записываем все элементы
        foreach (var (k, data) in elements)
        {
            queue.Enqueue(k, data);
        }

        // Читаем все элементы
        var readElements = queue.DequeueAll();

        readElements
           .Should()
           .Equal(elements, "Порядок прочитанных сообщений должен соответствовать порядку добавления в очередь");
    }

    [Fact]
    public void TryDequeue__КогдаВОчередиНесколькоРазныхКлючей__ДолженВозвращатьЭлеметныСНаибольшимПриоритетом()
    {
        var expected = ( Key: -9L, Data: new byte[] {6, 6, 3, 2} );
        var elements = new[]
        {
            ( -8L, RandomData() ), ( 5L, RandomData() ), ( 0L, RandomData() ), ( 10L, RandomData() ), expected,
            ( 1L, RandomData() ),
        };
        var queue = CreateQueue(-10L, 10L, elements);

        foreach (var (key, data) in elements)
        {
            queue.Enqueue(key, data);
        }

        _ = queue.TryDequeue(out var actualKey, out var actualData);

        actualKey
           .Should()
           .Be(expected.Key, "Должна быть получена запись с минимальным ключом");

        actualData
           .Should()
           .Equal(expected.Data, "Полученные данные должны быть равны записанным");
    }

    [Fact]
    public void TryDequeue__СНепустойОчередью__ДолженВозвращатьЗаписиВПорядкеВозрастанияКлючей()
    {
        // В проядке убывания ключи расположены
        var elements = Enumerable.Range(0, 20)
                                 .Select(i => ( Key: 10L - i, Data: new byte[] {( byte ) i, ( byte ) ( i + 1 )} ))
                                 .ToArray();

        // Ожидаем их чтение в обратном порядке, т.е. по возрастанию
        var expected = elements.Reverse()
                               .ToArray();
        var queue = CreateQueue(-10L, 10L);

        foreach (var (key, data) in elements)
        {
            queue.Enqueue(key, data);
        }

        var actual = queue.DequeueAll();
        actual
           .Should()
           .Equal(expected, "Ключи должны быть расположены в порядке возрастания");
    }

    private static byte[] RandomData()
    {
        var buffer = new byte[Random.Shared.Next(0, 100)];
        Random.Shared.NextBytes(buffer);
        return buffer;
    }
}