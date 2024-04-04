using FluentAssertions;
using TaskFlux.PriorityQueue.QueueArray;
using Xunit;
using QueueArrayPriorityQueue = TaskFlux.PriorityQueue.QueueArray.QueueArrayPriorityQueue<int>;

namespace TaskFlux.PriorityQueue.Tests;

[Trait("Category", "BusinessLogic")]
public class QueueArrayPriorityQueueTests
{
    private static QueueArrayPriorityQueue CreateQueue(long min,
                                                       long max,
                                                       IEnumerable<(long, int)>? payload = null) =>
        payload is null
            ? new(min, max)
            : new(min, max, payload);

    private static readonly int StubPayload = 867654;

    [Fact]
    public void Enqueue__КогдаКлюч0_ДиапазонМни10До10__ДолженВставитьЭлемент()
    {
        var queue = CreateQueue(-10, 10);
        var data = 24562456;
        var priority = 0L;

        queue.Enqueue(priority, data);

        var allData = queue.ToListUnordered();
        allData
           .Should()
           .ContainSingle(x => x.Item1 == priority && x.Item2 == data,
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
               .ContainSingle(x => x.Item1 == minKey && x.Item2 == payload,
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
               .ContainSingle(x => x.Item1 == maxKey && x.Item2 == payload,
                    "Максимальный ключ должен включаться");
        }
    }

    [Fact]
    public void Enqueue__КогдаКлючВыходитЗаПределыДиапазона__ДолженКинутьИсключение()
    {
        var minKey = -25L;
        var maxKey = 100L;
        var priority = 101L;
        var queue = CreateQueue(minKey, maxKey);

        // Пока не определился нужно ли задавать конкретное исключение, 
        // поэтому сейчас будем кидать любое исключение 
        Assert.ThrowsAny<InvalidKeyRangeException>(() => queue.Enqueue(priority, StubPayload));
    }

    [Fact]
    public void TryDequeue__КогдаОчередьПуста__ДолженВернутьFalse()
    {
        var queue = CreateQueue(-1, 1);

        var success = queue.TryDequeue(out _, out _);

        success
           .Should()
           .BeFalse("очередь пуста");
    }

    [Fact]
    public void TryDequeue__КогдаВОчереди1Элемент__ДолженВернутьЭтотЭлемент()
    {
        var priority = 0L;
        var payload = 75645;

        var record = ( priority, payload );
        var queue = CreateQueue(-1, 1, new[] {record});

        var success = queue.TryDequeue(out var actualPriority, out var actualPayload);

        success
           .Should()
           .BeTrue("Очередь не была пуста");

        actualPriority
           .Should()
           .Be(priority, "Вставленный и полученный ключи должны быть одинаковы");

        actualPayload
           .Should()
           .Be(payload, "Хранящиеся данные не должны быть измненены");
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
        var random = new Random(111);
        var elements = Enumerable.Range(0, elementsCount)
                                 .Select(_ => ( Priority: random.NextInt64(min, max + 1), Data: random.Next() ))
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
    public void ЧтениеСОднимИТемЖеКлючом__ДолженВозвращатьЭлементыВПорядкеДобавления()
    {
        var key = 1L;
        var elements = new (long, int)[] {( key, 1 ), ( key, 2 ), ( key, 3 ), ( key, 4 ), ( key, 5 ),};
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
        var expected = ( Priority: -9L, Data: 8765 );
        var random = new Random(-1);
        var elements = new[]
        {
            ( -8L, random.Next() ), ( 5L, random.Next() ), ( 0L, random.Next() ), ( 10L, random.Next() ), expected,
            ( 1L, random.Next() ),
        };
        var queue = CreateQueue(-10L, 10L, elements);

        foreach (var (key, data) in elements)
        {
            queue.Enqueue(key, data);
        }

        _ = queue.TryDequeue(out var actualKey, out var actualData);

        actualKey
           .Should()
           .Be(expected.Priority, "Должна быть получена запись с минимальным ключом");

        actualData
           .Should()
           .Be(expected.Data, "Полученные данные должны быть равны записанным");
    }

    [Fact]
    public void TryDequeue__СНепустойОчередью__ДолженВозвращатьЗаписиВПорядкеВозрастанияКлючей()
    {
        // В проядке убывания ключи расположены
        var elements = Enumerable.Range(0, 20)
                                 .Select(i => ( Priority: 10L - i, Data: i ))
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
}