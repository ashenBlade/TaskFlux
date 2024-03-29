using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Core.Restore;
using TaskFlux.Persistence.ApplicationState;
using TaskFlux.Persistence.ApplicationState.Deltas;
using TaskFlux.PriorityQueue;
using Xunit;

namespace TaskFlux.Persistence.Tests;

[Trait("Category", "Serialization")]
public class StateRestorerTests
{
    private static readonly IEqualityComparer<QueueInfo> Comparer = QueueInfoEqualityComparer.Instance;
    private static readonly ISnapshot ZeroQueuesSnapshot = new QueueCollectionSnapshot(new QueueCollection());

    [Fact]
    public void RestoreState__КогдаСнапшотаИДельтНет__ДолженВернутьОднуОчередьПоУмолчанию()
    {
        var expected = Defaults.CreateDefaultQueueInfo();

        var collection = StateRestorer.RestoreState(null, Array.Empty<byte[]>());

        var queues = collection.GetAllQueues();
        Assert.Single(queues);
        var actual = queues.Single();
        Assert.Equal(expected, actual, Comparer);
    }

    private static QueueInfo CreateQueueInfo(QueueName name,
                                             PriorityQueueCode code,
                                             int? maxQueueSize,
                                             int? maxPayloadSize,
                                             (long, long)? priorityRange,
                                             IEnumerable<QueueRecord> records)
    {
        var info = new QueueInfo(name, code, maxQueueSize, maxPayloadSize, priorityRange);
        foreach (var (priority, payload) in records)
        {
            info.Add(priority, payload);
        }

        return info;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(128)]
    [InlineData(512)]
    public void RestoreState__КогдаТолькоОперацииДобавленияВОчередьПоУмолчанию__ДолженПрименитьОперации(
        int insertsCount)
    {
        var random = new Random(745643);
        var records = Enumerable.Range(0, insertsCount)
                                .Select(_ => new QueueRecord(random.NextInt64(), random.ByteArray(32)))
                                .ToArray();
        var expected = CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null, records);
        var deltas = records.Select(r => new AddRecordDelta(QueueName.Default, r.Priority, r.Payload)).ToArray();
        random.Shuffle(deltas);
        var actual = StateRestorer.RestoreState(null, deltas.Select(d => d.Serialize()))
                                  .GetAllQueues()
                                  .ToHashSet(Comparer);

        Assert.Equal(new[] {expected}.ToHashSet(Comparer), actual, Comparer);
    }

    [Theory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(2000)]
    [InlineData(5000)]
    [InlineData(10000)]
    public void RestoreState__КогдаДобавляютсяЗаписиСОдинаковымКлючом__ДолженПрименитьОперации(int recordsCount)
    {
        var random = new Random(675645324);
        const long priority = 1L;
        var records = Enumerable.Range(0, recordsCount)
                                .Select(_ => new QueueRecord(priority, random.ByteArray(128)))
                                .ToArray();
        var queueInfo = CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null, records);
        var deltas = records
                    .Select(record => new AddRecordDelta(QueueName.Default, priority, record.Payload).Serialize())
                    .ToArray();
        var expected = new[] {queueInfo}.ToHashSet(Comparer);

        var actual = StateRestorer.RestoreState(null, deltas)
                                  .GetAllQueues()
                                  .ToHashSet(Comparer);

        Assert.Single(actual);
        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(2000)]
    [InlineData(5000)]
    [InlineData(10000)]
    public void RestoreState__КогдаДобавляютсяЗаписиСОдинаковымиКлючамиИОдинаковымиДанными__ДолженПрименитьОперации(
        int recordsCount)
    {
        const long priority = 1L;
        var random = new Random(87347834);
        var data = random.ByteArray(64);
        var record = new QueueRecord(priority, data);
        var queueInfo = CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null,
            new[] {record});
        var expected = new[] {queueInfo}.ToHashSet(Comparer);

        var actual = StateRestorer.RestoreState(null, Enumerable.Repeat(data, recordsCount)
                                                                .Select(d =>
                                                                     new AddRecordDelta(QueueName.Default, priority, d)
                                                                        .Serialize()))
                                  .GetAllQueues()
                                  .ToHashSet(Comparer);

        Assert.Single(actual);
        Assert.Equal(actual, expected, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаЗаписиДобавляютсяИУдаляются__ДолженПрименятьОперацииПравильно()
    {
        var records = new QueueRecord[]
        {
            new(1L, "data1"u8.ToArray()), new(-100L, "hello, world"u8.ToArray()),
            new(10000000L, "what is "u8.ToArray()), new(10000000L, "what is "u8.ToArray()),
            new(10000000L, "another value"u8.ToArray()), new(long.MaxValue, "asg345gqe4g(*^#%#"u8.ToArray()),
            new(long.MinValue, "234t(*&Q@%w34t"u8.ToArray()),
        };
        var queueInfo = CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null, records);
        var expected = new[] {queueInfo}.ToHashSet(Comparer);

        var actual = StateRestorer.RestoreState(null, new Delta[]
                                       {
                                           new AddRecordDelta(QueueName.Default, 10L, "hello, onichan"u8.ToArray()),
                                           new RemoveRecordDelta(QueueName.Default, 10L,
                                               "hello, onichan"u8.ToArray()),
                                           new AddRecordDelta(QueueName.Default, 1L, "data1"u8.ToArray()),
                                           new AddRecordDelta(QueueName.Default, 1L, "data1"u8.ToArray()),
                                           new RemoveRecordDelta(QueueName.Default, 1L, "data1"u8.ToArray()),
                                           new AddRecordDelta(QueueName.Default, -100L, "hello, world"u8.ToArray()),
                                           new AddRecordDelta(QueueName.Default, 10000000L, "what is "u8.ToArray()),
                                           new AddRecordDelta(QueueName.Default, 10000000L, "what is "u8.ToArray()),
                                           new AddRecordDelta(QueueName.Default, 10000000L,
                                               "another value"u8.ToArray()),
                                           new AddRecordDelta(QueueName.Default, long.MaxValue,
                                               "asg345gqe4g(*^#%#"u8.ToArray()),
                                           new AddRecordDelta(QueueName.Default, long.MinValue,
                                               "234t(*&Q@%w34t"u8.ToArray()),
                                       }
                                      .Select(x => x.Serialize()))
                                  .GetAllQueues();

        Assert.Single(actual);
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаЕстьОперацияУдаленияОчередиИзСнапшота__ДолженУдалитьОчередь()
    {
        var queueToDelete = QueueName.Parse("sample_queue");

        var records =
            new QueueRecord[] {new(1L, "aasdfasdf"u8.ToArray()), new(100L, "vasdaedhraerqa(Q#%V"u8.ToArray())};
        var snapshotQueues = new ITaskQueue[]
        {
            new StubTaskQueue(queueToDelete, PriorityQueueCode.Heap4Arity, null, null, null, records),
            // По умолчанию
            new StubTaskQueue(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null,
                Array.Empty<QueueRecord>()),
        };
        var expected = new[]
        {
            CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null,
                Array.Empty<QueueRecord>())
        }.ToHashSet(Comparer);

        var actual = StateRestorer.RestoreState(new QueueArraySnapshot(snapshotQueues),
                                   new Delta[] {new DeleteQueueDelta(queueToDelete),}
                                      .Select(x => x.Serialize()))
                                  .GetAllQueues()
                                  .ToHashSet(Comparer);

        Assert.Single(actual);
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаЕстьСнапшотИЛогПустой__ДолженВернутьОчередиИзСнапшота()
    {
        var snapshotQueues = new StubTaskQueue[]
        {
            new(QueueName.Parse("sample_queue"), PriorityQueueCode.Heap4Arity, null, null, null,
                new QueueRecord[] {new(1L, "aasdfasdf"u8.ToArray()), new(100L, "vasdaedhraerqa(Q#%V"u8.ToArray())}),
            new(QueueName.Parse("orders:1002"), PriorityQueueCode.QueueArray, 100000, ( -10L, 10L ), null,
                new QueueRecord[]
                {
                    new(9L, "hello, world"u8.ToArray()), new(9L, "hello, world"u8.ToArray()),
                    new(9L, "hello, world"u8.ToArray()), new(9L, "hello, world"u8.ToArray()),
                }),
            new(QueueName.Parse("___@Q#%GWSA"), PriorityQueueCode.Heap4Arity, null, null, 123123,
                Array.Empty<QueueRecord>())
        };
        var expected = snapshotQueues.Select(tq => CreateQueueInfo(tq.Name, tq.Code, tq.Metadata.MaxQueueSize,
                                          tq.Metadata.MaxPayloadSize, tq.Metadata.PriorityRange, tq.ReadAllData()))
                                     .ToHashSet(Comparer);

        var actual = StateRestorer.RestoreState(new QueueArraySnapshot(snapshotQueues), Array.Empty<byte[]>())
                                  .GetAllQueues()
                                  .ToHashSet(Comparer);

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    public void RestoreState__КогдаЕстьОперацииСозданияНовойОчереди__ДолженСоздатьНовыеОчереди(int newQueuesCount)
    {
        var expected = Enumerable.Range(0, newQueuesCount)
                                 .Select(i => CreateQueueInfo(QueueName.Parse(i.ToString()),
                                      PriorityQueueCode.Heap4Arity, i, null, null, Array.Empty<QueueRecord>()))
                                 .ToHashSet(Comparer);

        var actual = StateRestorer.RestoreState(ZeroQueuesSnapshot,
                                   expected.Select(qi =>
                                       new CreateQueueDelta(qi.QueueName, qi.Code, qi.MaxQueueSize, qi.MaxPayloadSize,
                                               qi.PriorityRange)
                                          .Serialize()))
                                  .GetAllQueues()
                                  .ToHashSet(Comparer);

        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаОчередьСоздаетсяИУдаляется__ДолженУдалитьОчередь()
    {
        var queueToDelete = CreateQueueInfo(QueueName.Parse("hello_world"), PriorityQueueCode.Heap4Arity, 15343, null,
            ( -10, 10 ), Array.Empty<QueueRecord>());

        var actual = StateRestorer.RestoreState(ZeroQueuesSnapshot,
                                   new Delta[]
                                   {
                                       new CreateQueueDelta(queueToDelete.QueueName, queueToDelete.Code,
                                           queueToDelete.MaxQueueSize, queueToDelete.MaxPayloadSize,
                                           queueToDelete.PriorityRange),
                                       new DeleteQueueDelta(queueToDelete.QueueName),
                                   }.Select(i => i.Serialize()))
                                  .GetAllQueues();

        Assert.DoesNotContain(queueToDelete, actual, Comparer);
    }
}