using FluentAssertions;
using TaskFlux.Application.Persistence;
using TaskFlux.Application.Persistence.Delta;
using TaskFlux.Core;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Application.Tests;

[Trait("Category", "Serialization")]
public class DeltaTests
{
    private static void AssertSerializationBase(Delta expected)
    {
        var data = expected.Serialize();
        var actual = Delta.DeserializeFrom(data);
        Assert.Equal(expected, actual, DeltaEqualityComparer.Instance);
    }

    public static IEnumerable<object?[]> CreateQueueDeltaSerialization => new[]
    {
        new object?[] {"", 0, 0, 0, null},
        new object?[] {"hello,world", 123, 123321, 12415, ( 14124L, 1423523452L )},
        new object?[] {"orders:1:2023", 1, null, null, null},
        new object?[] {"USERS_KNOWN________&", 10, 100, 1024 * 2, ( -10L, 10L )},
    };

    [Theory]
    [MemberData(nameof(CreateQueueDeltaSerialization))]
    public void CreateQueueDelta__Serialization(string queueName,
                                                int implementation,
                                                int maxQueueSize,
                                                int maxMessageSize,
                                                (long, long)? priorityRange)
    {
        AssertSerializationBase(new CreateQueueDelta(QueueNameParser.Parse(queueName),
            ( PriorityQueueCode ) implementation,
            maxQueueSize, maxMessageSize,
            priorityRange));
    }

    [Theory]
    [InlineData("queue")]
    [InlineData("asdfasdfasf2q9it4y-8tIU{^YOIUY{P*W#")]
    [InlineData("orders:1:2:___")]
    [InlineData("")]
    public void DeleteQueueDelta__Serialization(string queueName)
    {
        AssertSerializationBase(new DeleteQueueDelta(QueueNameParser.Parse(queueName)));
    }

    [Theory]
    [InlineData("", 0L, new byte[] { })]
    [InlineData("", 123123L, new byte[] {1, 2, 3, 4, 5, 6, 7})]
    [InlineData("hello,world", long.MaxValue, new byte[] {255, 0, 1, 34, 66})]
    public void AddRecordDelta__Serialization(string queueName, long key, byte[] message)
    {
        AssertSerializationBase(new AddRecordDelta(QueueNameParser.Parse(queueName), key, message));
    }

    [Theory]
    [InlineData("", 0L, new byte[] { })]
    [InlineData("", 123123L, new byte[] {1, 2, 3, 4, 5, 6, 7})]
    [InlineData("hello,world", long.MaxValue, new byte[] {255, 0, 1, 34, 66})]
    public void RemoveRecordDelta__Serialization(string queueName, long key, byte[] message)
    {
        AssertSerializationBase(new RemoveRecordDelta(QueueNameParser.Parse(queueName), key, message));
    }

    [Theory]
    [InlineData("")]
    [InlineData("hello,world")]
    [InlineData("orders:1")]
    public void CreateQueueDelta__Apply__КогдаОчередиНеБыло__ДолженСоздатьНовуюОчередь(string queueName)
    {
        var collection = new QueueCollection();
        var name = QueueName.Parse(queueName);
        var delta = new CreateQueueDelta(name, PriorityQueueCode.Heap4Arity, null, null, null);

        delta.Apply(collection);

        var queues = collection.GetQueuesRaw();

        queues
           .Should()
           .NotBeEmpty("очередь должна быть создана")
           .And
           .Contain(q => q.Name == name, "должна быть создана очередь с указанным названием");
    }

    public static IEnumerable<object[]> AllPriorityCodes =>
        Enum.GetValues<PriorityQueueCode>().Select(x => new object[] {x});

    [Theory]
    [MemberData(nameof(AllPriorityCodes))]
    public void CreateQueueDelta__ДолженСоздатьОчередьСУказаннымТипом(PriorityQueueCode code)
    {
        var collection = new QueueCollection();
        var delta = new CreateQueueDelta(QueueName.Default, code, null, null, null);

        delta.Apply(collection);

        var queues = collection.GetQueuesRaw();

        queues
           .Should()
           .Contain(q => q.Code == code, "очередь должна быть указанного типа");
    }

    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(1024)]
    [InlineData(1024 * 2)]
    public void CreateQueueDelta__ДолженУказатьПравильныйMaxQueueSize(int? maxQueueSize)
    {
        var collection = new QueueCollection();
        var delta = new CreateQueueDelta(QueueName.Default, PriorityQueueCode.Heap4Arity, maxQueueSize, null, null);

        delta.Apply(collection);

        var queues = collection.GetQueuesRaw();

        queues
           .Should()
           .Contain(q => q.MaxQueueSize == maxQueueSize, "максимальный размер очереди должен быть равен указанному");
    }

    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(1024)]
    [InlineData(1024 * 2)]
    public void CreateQueueDelta__ДолженУказатьПравильныйMaxMessageSize(int? maxMessageSize)
    {
        var collection = new QueueCollection();
        var delta = new CreateQueueDelta(QueueName.Default, PriorityQueueCode.Heap4Arity, null, maxMessageSize, null);

        delta.Apply(collection);

        var queues = collection.GetQueuesRaw();

        queues
           .Should()
           .Contain(q => q.MaxPayloadSize == maxMessageSize,
                "максимальный размер сообщения должен быть равен указанному");
    }

    public static object?[][] PriorityRanges => new[]
    {
        new object?[] {null}, new object?[] {( 1L, 2L )}, new object?[] {( long.MinValue, long.MaxValue )},
        new object?[] {( -1000L, 1002L )},
    };

    [Theory]
    [MemberData(nameof(PriorityRanges))]
    public void CreateQueueDelta__ДолженУказатьПравильныйPriorityRange((long, long)? range)
    {
        var collection = new QueueCollection();
        var delta = new CreateQueueDelta(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, range);

        delta.Apply(collection);

        var queues = collection.GetQueuesRaw();

        var (_, _, _, _, priorityRange, _) = queues.Single();
        priorityRange.Should()
                     .Be(range, "диапазон должен быть равен указанному");
    }

    [Fact]
    public void DeleteQueueDelta__Apply__ДолженУдалитьОчередь()
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            Array.Empty<QueueRecord>());

        var delta = new DeleteQueueDelta(queueName);
        delta.Apply(collection);

        collection
           .GetQueuesRaw()
           .Should()
           .NotContain(x => x.Name == queueName, "очередь должна быть удалена");
    }

    [Fact]
    public void AddRecordDelta__Apply__КогдаОчередьПуста__ДолженДобавитьНовуюЗапись()
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            Array.Empty<QueueRecord>());

        var record = ( Key: 1L, Message: "asdfasdfgawpogiuahw"u8.ToArray() );
        var delta = new AddRecordDelta(queueName, record.Key, record.Message);
        delta.Apply(collection);

        var (_, _, _, _, _, data) = collection.GetQueuesRaw().Single(x => x.Name == queueName);
        data.Should()
            .ContainSingle(x => x.Equals(record));
    }

    [Fact]
    public void AddRecordDelta__Apply__КогдаВОчередиЕстьЭлементы__ДолженДобавитьНовуюЗапись()
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            new QueueRecord[] {new(100, "hello, world!"u8.ToArray()), new(-100, "what is going on?"u8.ToArray())});

        var record = ( Key: 1L, Message: "asdfasdfgawpogiuahw"u8.ToArray() );
        var delta = new AddRecordDelta(queueName, record.Key, record.Message);
        delta.Apply(collection);

        var (_, _, _, _, _, data) = collection.GetQueuesRaw().Single(x => x.Name == queueName);
        data.Should()
            .Contain(x => x.Equals(record), "запись должна быть добавлена");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public void AddRecordDelta__Apply__КогдаВОчередиЕстьЭлементыСТакимЖеКлючом__ДолженДобавитьНовуюЗапись(
        int existingCount)
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        const long key = 1L;
        var oldRecord = ( Key: key, Message: "hello, world!"u8.ToArray() );
        var existingRecords = Enumerable.Range(0, existingCount)
                                        .Select(i => new QueueRecord(oldRecord.Key, Random.Shared.RandomBuffer(i)))
                                        .ToArray();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null, existingRecords);

        var newRecord = oldRecord with {Message = "asdfasdfgawpogiuahw"u8.ToArray()};
        var delta = new AddRecordDelta(queueName, newRecord.Key, newRecord.Message);
        delta.Apply(collection);

        var (_, _, _, _, _, data) = collection.GetQueuesRaw().Single(x => x.Name == queueName);
        data.Where(x => x.Key == key)
            .Should()
            .Contain(x => x.Key == newRecord.Key && x.Payload.SequenceEqual(newRecord.Message),
                 "запись должна быть добавлена")
            .And
            .HaveCount(existingCount + 1, "было {0} и добавили 1", existingCount)
            .And
            .Contain(existingRecords.Select(x => ( x.Key, x.Message )).Append(newRecord));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(1000)]
    public void AddRecordDelta__Apply__КогдаВОчередиЕстьЭлементыСТакимЖеКлючомИСообщением__ДолженДобавитьНовуюЗапись(
        int existingCount)
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        var record = ( Key: 1L, Message: "hello, world!"u8.ToArray() );
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            Enumerable.Repeat(record, existingCount).Select(x => new QueueRecord(x.Key, x.Message)).ToArray());

        var delta = new AddRecordDelta(queueName, record.Key, record.Message);
        delta.Apply(collection);
        var (_, _, _, _, _, data) = collection.GetQueuesRaw().Single(x => x.Name == queueName);

        data.Where(x => x.Key == record.Key && x.Payload.SequenceEqual(record.Message))
            .Should()
            .HaveCount(existingCount + 1, "было {0} записей и добавили 1", existingCount);
    }

    [Fact]
    public void RemoveRecordDelta__Apply__КогдаВОчередиБылаОднаЗапись__ТакихЗаписейНеДолжноОстаться()
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        var record = ( Key: 1L, Message: "asdfasdfgawpogiuahw"u8.ToArray() );
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            new QueueRecord[] {new QueueRecord(record.Key, record.Message)});

        var delta = new RemoveRecordDelta(queueName, record.Key, record.Message);
        delta.Apply(collection);

        var (_, _, _, _, _, data) = collection.GetQueuesRaw().Single(x => x.Name == queueName);
        data.Should()
            .NotContain(x => x.Key == record.Key && x.Payload.SequenceEqual(record.Message),
                 "эта запись должна быть удалена");
    }

    [Theory]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public void RemoveRecordDelta__Apply__КогдаВОчередиБылоНесколькоЗаписей__ТакихЗаписейДолжноОстатьсяНа1Меньше(
        int existingCount)
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        var record = ( Key: 1L, Message: "asdfasdfgawpogiuahw"u8.ToArray() );
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            Enumerable.Range(0, existingCount).Select(_ => new QueueRecord(record.Key, record.Message)).ToArray());

        var delta = new RemoveRecordDelta(queueName, record.Key, record.Message);
        delta.Apply(collection);

        var (_, _, _, _, _, data) = collection.GetQueuesRaw()
                                              .Single(x => x.Name == queueName);
        data.Where(x => x.Key == record.Key && x.Payload.SequenceEqual(record.Message))
            .Should()
            .HaveCount(existingCount - 1, "было {0} записей и 1 удалена", existingCount);
    }
}