using System.Text;
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

    private static QueueRecord CreateRecord(int id, long priority, string data) =>
        new(new RecordId(( ulong ) id), priority, Encoding.UTF8.GetBytes(data));

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
                                             RecordId lastId,
                                             int? maxQueueSize,
                                             int? maxPayloadSize,
                                             (long, long)? priorityRange,
                                             IEnumerable<QueueRecord> records)
    {
        var queueInfo = new QueueInfo(name, code, lastId, maxQueueSize, maxPayloadSize, priorityRange);
        queueInfo.SetDataTest(records);
        return queueInfo;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(128)]
    [InlineData(512)]
    public void RestoreState__КогдаТолькоОперацииДобавленияВОчередьПоУмолчанию__ДолженПрименитьОперации(
        int recordsCount)
    {
        var lastId = new RecordId(( ulong ) recordsCount);
        var random = new Random(745643);
        var records = Enumerable.Range(1, recordsCount)
                                .Select(i => new QueueRecord(new RecordId(( ulong ) i), random.NextInt64(),
                                     random.ByteArray(32)))
                                .ToArray();
        var expected = CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, lastId, null, null, null,
            records);
        var deltas = records.Select(r => new AddRecordDelta(QueueName.Default, r.Priority, r.Payload).Serialize())
                            .ToArray();
        random.Shuffle(deltas);

        var actual = StateRestorer.RestoreState(null, deltas)
                                  .GetAllQueues()
                                  .Single();

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(2000)]
    [InlineData(5000)]
    [InlineData(10000)]
    public void RestoreState__КогдаДобавляютсяЗаписиСОдинаковымКлючом__ДолженПрименитьОперации(int recordsCount)
    {
        const long priority = 1L;

        var lastId = new RecordId(( ulong ) recordsCount);
        var random = new Random(675645324);
        var records = Enumerable.Range(1, recordsCount)
                                .Select(i =>
                                     new QueueRecord(new RecordId(( ulong ) i), priority, random.ByteArray(128)))
                                .ToArray();
        var expected = CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, lastId, null, null, null,
            records);

        var actual = StateRestorer.RestoreState(null,
                                   records.Select(record =>
                                       new AddRecordDelta(QueueName.Default, record.GetData()).Serialize()))
                                  .GetAllQueues()
                                  .Single();

        Assert.Equal(expected, actual, Comparer);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(2000)]
    [InlineData(5000)]
    [InlineData(10000)]
    public void RestoreState__КогдаДобавляютсяЗаписиСОдинаковымиКлючамиИОдинаковымиДанными__ДолженПрименитьОперации(
        int recordsCount)
    {
        var lastId = new RecordId(( ulong ) recordsCount);
        const long priority = 1L;
        var random = new Random(87347834);
        var data = random.ByteArray(64);

        var records = Enumerable.Range(1, recordsCount)
                                .Select(i => new QueueRecord(new RecordId(( ulong ) i), priority, data))
                                .ToArray();

        var queueInfo = CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, lastId, null, null, null,
            records);
        var actual = StateRestorer
                    .RestoreState(null,
                         records.Select(r => new AddRecordDelta(QueueName.Default, r.GetData()).Serialize()))
                    .GetAllQueues()
                    .Single();

        Assert.Equal(actual, queueInfo, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаЗаписиДобавляютсяИУдаляются__ДолженПрименятьОперацииПравильно()
    {
        var resultRecords = new QueueRecord[]
        {
            CreateRecord(2, 1L, "data1"), CreateRecord(4, -100L, "hello, world"),
            CreateRecord(5, 10000000L, "what is "), CreateRecord(7, 10000000L, "another value"),
            CreateRecord(8, long.MaxValue, "asg345gqe4g(*^#%#"), CreateRecord(9, long.MinValue, "234t(*&Q@%w34t"),
        };

        var expected = CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, new RecordId(9), null, null,
            null, resultRecords);

        var deltas = new Delta[]
        {
            // 1
            new AddRecordDelta(QueueName.Default, new QueueRecordData(10L, "hello, onichan"u8.ToArray())),
            // 2
            new AddRecordDelta(QueueName.Default, new QueueRecordData(1L, "data1"u8.ToArray())),
            // 3
            new AddRecordDelta(QueueName.Default, new QueueRecordData(1L, "data1"u8.ToArray())),
            new RemoveRecordDelta(QueueName.Default, new RecordId(3)),
            // 4
            new AddRecordDelta(QueueName.Default, new QueueRecordData(-100L, "hello, world"u8.ToArray())),
            // 5
            new AddRecordDelta(QueueName.Default, new QueueRecordData(10000000L, "what is "u8.ToArray())),
            // 6
            new AddRecordDelta(QueueName.Default, new QueueRecordData(10000000L, "what is "u8.ToArray())),
            // 7
            new AddRecordDelta(QueueName.Default, new QueueRecordData(10000000L, "another value"u8.ToArray())),
            // 8
            new AddRecordDelta(QueueName.Default, new QueueRecordData(long.MaxValue, "asg345gqe4g(*^#%#"u8.ToArray())),
            // 9
            new AddRecordDelta(QueueName.Default, new QueueRecordData(long.MinValue, "234t(*&Q@%w34t"u8.ToArray())),
            new RemoveRecordDelta(QueueName.Default, new RecordId(6)),
            new RemoveRecordDelta(QueueName.Default, new RecordId(1)),
        };
        var actual = StateRestorer.RestoreState(null, deltas.Select(x => x.Serialize()))
                                  .GetAllQueues()
                                  .Single();

        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаЕстьОперацияУдаленияОчередиИзСнапшота__ДолженУдалитьОчередь()
    {
        var queueToDelete = QueueName.Parse("sample_queue");

        var records =
            new QueueRecord[]
            {
                new(new RecordId(1), 1L, "aasdfasdf"u8.ToArray()),
                new(new RecordId(2), 100L, "vasdaedhraerqa(Q#%V"u8.ToArray())
            };
        var snapshotQueues = new ITaskQueue[]
        {
            new StubTaskQueue(queueToDelete, PriorityQueueCode.Heap4Arity, new RecordId(2), null, null, null, records),
            // По умолчанию
            new StubTaskQueue(QueueName.Default, PriorityQueueCode.Heap4Arity, new RecordId(424), null, null, null,
                Array.Empty<QueueRecord>()),
        };
        var expected = new[]
        {
            CreateQueueInfo(QueueName.Default, PriorityQueueCode.Heap4Arity, new RecordId(424), null, null, null,
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
            new(QueueName.Parse("sample_queue"), PriorityQueueCode.Heap4Arity, new RecordId(23456), null, null, null,
                new QueueRecord[]
                {
                    new(new RecordId(1222), 1L, "aasdfasdf"u8.ToArray()),
                    new(new RecordId(23455), 100L, "vasdaedhraerqa(Q#%V"u8.ToArray())
                }),
            new(QueueName.Parse("orders:1002"), PriorityQueueCode.QueueArray, new RecordId(123), 100000, ( -10L, 10L ),
                null,
                new QueueRecord[]
                {
                    new(new RecordId(100), 9L, "hello, world"u8.ToArray()),
                    new(new RecordId(101), 9L, "hello, world"u8.ToArray()),
                    new(new RecordId(102), 9L, "hello, world"u8.ToArray()),
                    new(new RecordId(103), 9L, "hello, world"u8.ToArray()),
                }),
            new(QueueName.Parse("___@Q#%GWSA"), PriorityQueueCode.Heap4Arity, new RecordId(0), null, null,
                123123,
                Array.Empty<QueueRecord>())
        };
        var expected = snapshotQueues.Select(tq => CreateQueueInfo(tq.Name, tq.Code, tq.LastId,
                                          tq.Metadata.MaxQueueSize,
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
                                      PriorityQueueCode.Heap4Arity, RecordId.Start, i, null, null,
                                      Array.Empty<QueueRecord>()))
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
        var queueToDelete = CreateQueueInfo(QueueName.Parse("hello_world"), PriorityQueueCode.Heap4Arity,
            RecordId.Start, 15343, null,
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