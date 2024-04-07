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
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    [InlineData(32457)]
    [InlineData(-32457)]
    public void AddRecordDelta__Priority__Serialization(long priority)
    {
        AssertSerializationBase(new AddRecordDelta(QueueName.Default,
            new QueueRecordData(priority, Array.Empty<byte>())));
    }

    [Theory]
    [InlineData(new byte[] { })]
    [InlineData(new byte[] {1})]
    [InlineData(new byte[] {1, 2, 3, 4, 5, 6, 7})]
    [InlineData(new byte[] {255, 0, 1, 34, 66})]
    public void AddRecordDelta__Payload__Serialization(byte[] payload)
    {
        AssertSerializationBase(new AddRecordDelta(QueueName.Default, new QueueRecordData(0, payload)));
    }

    public static IEnumerable<object[]> QueueNames => new object[][]
    {
        [QueueName.Default], [QueueName.Parse("a")], [new string('a', QueueNameParser.MaxNameLength)],
        [QueueName.Parse("asdfad_asgdaqwrgoi4h:q3hgcq3")]
    };

    [Theory]
    [MemberData(nameof(QueueNames))]
    public void AddRecordDelta__QueueName__Serialization(string name)
    {
        AssertSerializationBase(new AddRecordDelta(QueueName.Parse(name), new QueueRecordData(0, Array.Empty<byte>())));
    }

    [Theory]
    [MemberData(nameof(QueueNames))]
    public void RemoveRecordDelta__QueueName__Serialization(string queueName)
    {
        AssertSerializationBase(new RemoveRecordDelta(QueueNameParser.Parse(queueName), RecordId.Start));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(23)]
    [InlineData(ulong.MaxValue - 1)]
    [InlineData(ulong.MaxValue)]
    public void RemoveRecordDelta__Id__Serialization(ulong id)
    {
        AssertSerializationBase(new RemoveRecordDelta(QueueName.Default, new RecordId(id)));
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
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, RecordId.Start, null, null, null,
            Array.Empty<QueueRecord>());

        var delta = new DeleteQueueDelta(queueName);
        delta.Apply(collection);

        Assert.DoesNotContain(collection.GetAllQueues(), q => q.QueueName == queueName);
    }

    private static QueueRecord CreateRecord(int id, long priority, string payload) =>
        new(new(( ulong ) id), priority, Encoding.UTF8.GetBytes(payload));

    private static QueueRecord CreateRecord(int id, long priority, byte[] payload) =>
        new(new(( ulong ) id), priority, payload);

    [Fact]
    public void AddRecordDelta__Apply__КогдаОчередьПуста__ДолженДобавитьНовуюЗапись()
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, RecordId.Start, null, null, null,
            Array.Empty<QueueRecord>());
        var delta = new AddRecordDelta(queueName, new QueueRecordData(1L, "asdfasdfgawpogiuahw"u8.ToArray()));
        var expected = new QueueRecord(new RecordId(1), delta.Priority, delta.Payload);

        delta.Apply(collection);

        var info = collection.GetAllQueues().Single(x => x.QueueName == queueName);
        Assert.Single(info.Data, d => d.Equals(expected));
    }

    [Fact]
    public void AddRecordDelta__Apply__КогдаВОчередиЕстьЭлементы__ДолженДобавитьНовуюЗапись()
    {
        var queueName = QueueName.Parse("hello_world_queue");

        var collection = new QueueCollection();
        var existingRecords = new QueueRecord[] {CreateRecord(1, 123, "hasdfasdf"), CreateRecord(3, 35353, "aaaa"),};
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, new RecordId(3), null, null, null,
            existingRecords);
        var recordToAdd = new QueueRecord(new RecordId(4), 1L, "asdfasdfgawpogiuahw"u8.ToArray());

        var delta = new AddRecordDelta(queueName, recordToAdd.GetData());
        delta.Apply(collection);

        var info = collection.GetAllQueues().Single(x => x.QueueName == queueName);
        Assert.Contains(info.Data, d => d.Equals(recordToAdd));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public void AddRecordDelta__Apply__КогдаВОчередиЕстьЭлементыСТакимЖеПриоритетом__ДолженДобавитьНовуюЗапись(
        int existingCount)
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        var newRecord = CreateRecord(existingCount + 1, 123, "hello, world");
        var existingRecords = Enumerable.Range(1, existingCount)
                                        .Select(i => CreateRecord(i, newRecord.Priority, i.ToString()))
                                        .ToArray();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, new RecordId(( ulong ) existingCount),
            null, null, null, existingRecords);

        var delta = new AddRecordDelta(queueName, newRecord.GetData());
        delta.Apply(collection);

        var info = collection.GetAllQueues().Single(x => x.QueueName == queueName);

        var actualData = info.Data.Where(x => x.Priority == newRecord.Priority).ToArray();
        Assert.Contains(actualData, x => x.Equals(newRecord));
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
        var record = new QueueRecord(new RecordId(( ulong ) existingCount + 1), 1L, "hello, world!"u8.ToArray());
        var collection = new QueueCollection();
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, new RecordId(( ulong ) existingCount),
            null, null, null,
            Enumerable.Range(0, existingCount).Select(x => CreateRecord(x, record.Priority, record.Payload)).ToArray());

        var delta = new AddRecordDelta(queueName, record.GetData());
        delta.Apply(collection);

        var info = collection.GetAllQueues().Single(x => x.QueueName == queueName);
        Assert.Contains(info.Data, r => r == record);
    }

    [Fact]
    public void RemoveRecordDelta__Apply__КогдаВОчередиБылаЗапись__ЗаписьДолжнаУдалиться()
    {
        var queueName = QueueName.Parse("hello_world_queue");
        var collection = new QueueCollection();
        var record = new QueueRecord(new RecordId(1),
            1L, "asdfasdfgawpogiuahw"u8.ToArray());
        collection.AddExistingQueue(queueName, PriorityQueueCode.Heap4Arity, new RecordId(2), null, null, null,
            new[] {record});

        var delta = new RemoveRecordDelta(queueName, record.Id);
        delta.Apply(collection);

        var info = collection.GetAllQueues()
                             .Single(x => x.QueueName == queueName);
        Assert.DoesNotContain(info.Data, r => r == record);
    }
}