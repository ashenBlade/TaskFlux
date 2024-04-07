using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Persistence.ApplicationState;
using TaskFlux.PriorityQueue;
using Xunit;

namespace TaskFlux.Persistence.Tests;

[Trait("Category", "Serialization")]
public class QueueSnapshotSerializerTests
{
    private static void AssertBase(StubTaskQueue expected)
    {
        var serialized = QueuesSnapshotSerializer.Serialize(new[] {expected});
        var stream = new MemoryStream();
        foreach (var memory in serialized)
        {
            stream.Write(memory.Span);
        }

        stream.Position = 0;
        var collection = QueuesSnapshotSerializer.Deserialize(stream);
        var queues = collection.GetAllQueues();
        Assert.Single(queues);
        var queueInfo = queues.First();
        var actual = new StubTaskQueue(queueInfo.QueueName, queueInfo.Code, queueInfo.LastId, queueInfo.MaxQueueSize,
            queueInfo.PriorityRange, queueInfo.MaxPayloadSize, queueInfo.Data);
        Assert.Equal(expected, actual, TaskQueueEqualityComparer.Instance);
    }

    private static void AssertBase(IEnumerable<StubTaskQueue> queues)
    {
        var expected = queues.ToHashSet();
        var serialized = QueuesSnapshotSerializer.Serialize(expected);
        var stream = new MemoryStream();
        foreach (var memory in serialized)
        {
            stream.Write(memory.Span);
        }

        stream.Position = 0;

        var collection = QueuesSnapshotSerializer.Deserialize(stream);
        var actual = collection.GetAllQueues()
                               .Select(StubTaskQueue.FromQueueInfo)
                               .ToHashSet();
        Assert.Equal(expected, actual, TaskQueueEqualityComparer.Instance);
    }

    private static readonly QueueName DefaultName = QueueNameParser.Parse("hello");

    private static IEnumerable<QueueRecord> EmptyQueueData => Enumerable.Empty<QueueRecord>();

    [Fact]
    public void Serialize__КогдаПереданаПустаяОчередьБезПредела()
    {
        AssertBase(new StubTaskQueue(DefaultName, PriorityQueueCode.Heap4Arity, RecordId.Start, 0, null, null,
            EmptyQueueData));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(int.MaxValue)]
    [InlineData(100)]
    [InlineData(128)]
    [InlineData(1000)]
    [InlineData(98765423)]
    public void Serialize__КогдаПереданаПустаяОчередьСПределом(int limit)
    {
        AssertBase(new StubTaskQueue(DefaultName, PriorityQueueCode.Heap4Arity, RecordId.Start, limit, null, null,
            EmptyQueueData));
    }

    [Theory]
    [InlineData(0, new byte[0])]
    [InlineData(1, new byte[] {123})]
    [InlineData(-1, new[] {byte.MaxValue})]
    [InlineData(long.MaxValue, new byte[] {byte.MaxValue, 0, 0, 0, 0, 0, 0, 0})]
    [InlineData(long.MinValue, new byte[] {5, 65, 22, 75, 97, 32, 200})]
    [InlineData(123123, new byte[] {byte.MaxValue, 1, 2, 3, 4, 5, 6, byte.MinValue})]
    public void Serialize__КогдаПереданаОчередьС1ЭлементомБезПредела(long priority, byte[] data)
    {
        AssertBase(new StubTaskQueue(DefaultName, PriorityQueueCode.Heap4Arity, new(2), null, null, null,
            new QueueRecord[] {new(new(1), priority, data)}));
    }

    private static readonly Random Random = new(87);

    private IEnumerable<QueueRecord> CreateRandomQueueElements(int count, int startId)
    {
        var id = new RecordId(( ulong ) startId);
        for (int i = 0; i < count; i++)
        {
            var buffer = new byte[Random.Next(0, 100)];
            Random.NextBytes(buffer);
            yield return new QueueRecord(id, Random.NextInt64(), buffer);
            id = id.Increment();
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    [InlineData(20)]
    [InlineData(100)]
    [InlineData(128)]
    public void Serialize__КогдаЭлементовНесколько(int count)
    {
        AssertBase(new StubTaskQueue(DefaultName, PriorityQueueCode.QueueArray, new RecordId(( ulong ) count + 1), 0,
            null, null, CreateRandomQueueElements(count, 0)));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(20)]
    [InlineData(100)]
    public void Serialize__КогдаПереданоНесколькоПустыхОчередей__ДолженПравильноДесериализовать(int count)
    {
        var queues = Enumerable.Range(0, count)
                               .Select(i =>
                                    new StubTaskQueue(QueueName.CreateRandom(i),
                                        PriorityQueueCode.Heap4Arity,
                                        RecordId.Start,
                                        0,
                                        null, null,
                                        EmptyQueueData));
        AssertBase(queues);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(20)]
    public void Serialize__КогдаПереданоНесколькоНеПустыхОчередей__ДолженПравильноДесериализовать(int count)
    {
        var queues = Enumerable.Range(10, count)
                               .Select(i => new StubTaskQueue(QueueName.CreateRandom(i),
                                    PriorityQueueCode.QueueArray, new RecordId(( ulong ) i + 1), 0,
                                    null, null,
                                    CreateRandomQueueElements(i, 0)));
        AssertBase(queues);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(20)]
    public void Serialize__КогдаПереданоНесколькоОграниченныхОчередей__ДолженПравильноДесериализовать(
        int count)
    {
        var queues = Enumerable.Range(10, count)
                               .Select(i =>
                                {
                                    var limit = Random.Next(0, 255);
                                    var data = CreateRandomQueueElements(i, 0);
                                    return new StubTaskQueue(QueueName.CreateRandom(i),
                                        PriorityQueueCode.Heap4Arity, new RecordId(( ulong ) i), limit, null,
                                        null,
                                        data);
                                });
        AssertBase(queues);
    }

    [Theory]
    [InlineData(0L, 3L)]
    [InlineData(-10L, 10L)]
    [InlineData(long.MinValue, long.MaxValue)]
    [InlineData(0, 0)]
    [InlineData(long.MinValue / 2, long.MaxValue / 2)]
    [InlineData(0, long.MaxValue)]
    public void Serialize__КогдаПереданУказанныйДиапазонПриоритетов__ДолженПравильноДесериализовать(long min, long max)
    {
        AssertBase(new StubTaskQueue(QueueName.Default,
            PriorityQueueCode.Heap4Arity, RecordId.Start,
            maxSize: null,
            priority: ( min, max ),
            maxPayloadSize: null,
            elements: null));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(int.MaxValue)]
    [InlineData(1024 * 1024)]
    [InlineData(1024 * 1024 * 2)]
    public void Serialize__КогдаПереданМаксимальныйРазмерСообщения__ДолженПравильноДесериализовать(int maxPayloadSize)
    {
        AssertBase(new StubTaskQueue(QueueName.Default,
            PriorityQueueCode.QueueArray, RecordId.Start,
            maxSize: null,
            priority: null,
            maxPayloadSize: maxPayloadSize,
            elements: null));
    }
}