using System.Text;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Core.Restore;
using TaskFlux.Persistence.ApplicationState.Deltas;
using TaskFlux.PriorityQueue;
using Xunit;

namespace TaskFlux.Persistence.Tests;

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

        var queues = collection.GetAllQueues();

        Assert.NotEmpty(queues);
        Assert.Contains(queues, q => q.QueueName == name);
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

        var queues = collection.GetAllQueues();

        Assert.Contains(queues, q => q.Code == code);
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

        var queues = collection.GetAllQueues();

        Assert.Contains(queues, q => q.MaxQueueSize == maxQueueSize);
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

        var queues = collection.GetAllQueues();

        Assert.Contains(queues, q => q.MaxPayloadSize == maxMessageSize);
    }

    public static object?[][] PriorityRanges => new object?[][]
    {
        [null], [( 1L, 2L )], [( long.MinValue, long.MaxValue )], [( -1000L, 1002L )],
    };

    [Theory]
    [MemberData(nameof(PriorityRanges))]
    public void CreateQueueDelta__ДолженУказатьПравильныйPriorityRange((long, long)? range)
    {
        var collection = new QueueCollection();
        var delta = new CreateQueueDelta(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, range);

        delta.Apply(collection);

        var queues = collection.GetAllQueues();

        var info = queues.Single();
        Assert.Equal(range, info.PriorityRange);
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

        Assert.DoesNotContain(collection.GetAllQueues(), q => q.QueueName == queueName);
    }

    [Fact]
    public void AddRecordDelta__Apply__КогдаОчередьПуста__ДолженДобавитьНовуюЗапись()
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            Array.Empty<QueueRecord>());

        var record = new QueueRecord(1L, "asdfasdfgawpogiuahw"u8.ToArray());
        var delta = new AddRecordDelta(queueName, record.Priority, record.Payload);
        delta.Apply(collection);

        var info = collection.GetAllQueues().Single(x => x.QueueName == queueName);
        Assert.Single(info.Data, d => d.Equals(record));
    }

    [Fact]
    public void AddRecordDelta__Apply__КогдаВОчередиЕстьЭлементы__ДолженДобавитьНовуюЗапись()
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            new QueueRecord[] {new(100, "hello, world!"u8.ToArray()), new(-100, "what is going on?"u8.ToArray())});

        var record = new QueueRecord(1L, "asdfasdfgawpogiuahw"u8.ToArray());
        var delta = new AddRecordDelta(queueName, record.Priority, record.Payload);
        delta.Apply(collection);

        var info = collection.GetAllQueues().Single(x => x.QueueName == queueName);
        Assert.Contains(info.Data, d => d.Equals(record));
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
        const long priority = 1L;
        var oldRecord = ( Key: priority, Message: "hello, world!"u8.ToArray() );
        var existingRecords = Enumerable.Range(0, existingCount)
                                        .Select(i =>
                                             new QueueRecord(oldRecord.Key, Encoding.UTF8.GetBytes(i.ToString())))
                                        .ToArray();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null, existingRecords);

        (long Priority, byte[] Payload) newRecord = oldRecord with {Message = "asdfasdfgawpogiuahw"u8.ToArray()};
        var delta = new AddRecordDelta(queueName, newRecord.Priority, newRecord.Payload);
        delta.Apply(collection);

        var info = collection.GetAllQueues().Single(x => x.QueueName == queueName);

        var actualData = info.Data.Where(x => x.Priority == priority).ToArray();
        Assert.Contains(actualData,
            x => x.Priority == newRecord.Priority && x.Payload.SequenceEqual(newRecord.Payload));
        Assert.Equal(existingCount + 1, actualData.Length);
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
        var record = ( Priority: 1L, Payload: "hello, world!"u8.ToArray() );
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            Enumerable.Repeat(record, existingCount).Select(x => new QueueRecord(x.Priority, x.Payload)).ToArray());

        var delta = new AddRecordDelta(queueName, record.Priority, record.Payload);
        delta.Apply(collection);

        var info = collection.GetAllQueues().Single(x => x.QueueName == queueName);


        Assert.Equal(existingCount + 1,
            info.Data.Count(x => x.Priority == record.Priority && x.Payload.SequenceEqual(record.Payload)));
    }

    [Fact]
    public void RemoveRecordDelta__Apply__КогдаВОчередиБылаОднаЗапись__ТакихЗаписейНеДолжноОстаться()
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        var record = new QueueRecord(1L, "asdfasdfgawpogiuahw"u8.ToArray());
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            new QueueRecord[] {new(record.Priority, record.Payload)});

        var delta = new RemoveRecordDelta(queueName, record.Priority, record.Payload);
        delta.Apply(collection);

        var info = collection.GetAllQueues().Single(x => x.QueueName == queueName);
        Assert.DoesNotContain(info.Data, d => d.Priority == record.Priority && d.Payload.SequenceEqual(record.Payload));
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
        var record = new QueueRecord(1L, "asdfasdfgawpogiuahw"u8.ToArray());
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, null, null, null,
            Enumerable.Range(0, existingCount).Select(_ => new QueueRecord(record.Priority, record.Payload)).ToArray());

        var delta = new RemoveRecordDelta(queueName, record.Priority, record.Payload);
        delta.Apply(collection);

        var info = collection.GetAllQueues()
                             .Single(x => x.QueueName == queueName);

        Assert.Equal(existingCount - 1,
            info.Data.Count(d => d.Priority == record.Priority && d.Payload.SequenceEqual(record.Payload)));
    }
}