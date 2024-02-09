using System.Text;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Persistence.ApplicationState;
using TaskFlux.Persistence.ApplicationState.Deltas;
using TaskFlux.PriorityQueue;
using Xunit;

namespace TaskFlux.Persistence.Tests;

[Trait("Category", "Serialization")]
public class StateRestorerTests
{
    private static readonly IEqualityComparer<ITaskQueue> Comparer = TaskQueueEqualityComparer.Instance;

    [Fact]
    public void RestoreState__КогдаСнапшотаИДельтНет__ДолженВернутьОднуОчередьПоУмолчанию()
    {
        var expected = TaskQueueBuilder.CreateDefault();

        var state = StateRestorer.RestoreState(null, Array.Empty<byte[]>());

        var queues = state.BuildQueues();
        Assert.Single(queues);
        var actual = queues.Single();

        Assert.Equal(expected, actual, Comparer);
    }


    [Fact]
    public void RestoreState__КогдаТолькоОперацииДобавленияВОчередьПоУмолчанию__ДолженПрименитьОперации()
    {
        var expected = new ITaskQueue[]
        {
            new StubTaskQueue(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null,
                new (long, byte[])[]
                {
                    ( 1, new byte[] {1, 2, 3, 4, 5} ), ( 2, new byte[] {6, 66, 66, 6} ),
                    ( -1, new byte[] {40, 22, 11, 90} )
                })
        }.ToHashSet(Comparer);
        var actual = StateRestorer.RestoreState(null,
                                   new Delta[]
                                   {
                                       new AddRecordDelta(QueueName.Default, 1, new byte[] {1, 2, 3, 4, 5}),
                                       new AddRecordDelta(QueueName.Default, 2, new byte[] {6, 66, 66, 6}),
                                       new AddRecordDelta(QueueName.Default, -1, new byte[] {40, 22, 11, 90}),
                                   }.Select(d => d.Serialize()))
                                  .BuildQueues()
                                  .ToHashSet(Comparer);

        Assert.Equal(expected, actual, Comparer);
    }

    private static byte[] SerializeBytes(int value) => Encoding.UTF8.GetBytes(value.ToString());

    [Theory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(2000)]
    [InlineData(5000)]
    [InlineData(10000)]
    public void RestoreState__КогдаДобавляютсяЗаписиСОдинаковымКлючом__ДолженПрименитьОперации(int recordsCount)
    {
        var key = 1L;
        var allData = Enumerable.Range(0, recordsCount)
                                .Select(SerializeBytes)
                                .ToArray();
        var expected = new ITaskQueue[]
        {
            new StubTaskQueue(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null,
                allData.Select(d => ( key, d )))
        }.ToHashSet(Comparer);
        var actual = StateRestorer.RestoreState(null,
                                   allData.Select(data => new AddRecordDelta(QueueName.Default, key, data).Serialize()))
                                  .BuildQueues()
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
        const long key = 1L;
        var data = SerializeBytes(recordsCount);
        var expected = new ITaskQueue[]
        {
            new StubTaskQueue(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null,
                Enumerable.Repeat(( key, data ), recordsCount))
        }.ToHashSet(TaskQueueEqualityComparer.Instance);
        var actual = StateRestorer.RestoreState(null, Enumerable.Repeat(data, recordsCount)
                                                                .Select(d =>
                                                                     new AddRecordDelta(QueueName.Default, key, d)
                                                                        .Serialize()))
                                  .BuildQueues()
                                  .ToHashSet(TaskQueueEqualityComparer.Instance);

        Assert.Single(actual);
        Assert.Equal(actual, expected, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаЗаписиДобавляютсяИУдаляются__ДолженПрименятьОперацииПравильно()
    {
        var expected = new ITaskQueue[]
        {
            new StubTaskQueue(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null,
                new[]
                {
                    ( 1L, "data1"u8.ToArray() ), ( -100L, "hello, world"u8.ToArray() ),
                    ( 10000000L, "what is "u8.ToArray() ), ( 10000000L, "what is "u8.ToArray() ),
                    ( 10000000L, "another value"u8.ToArray() ), ( long.MaxValue, "asg345gqe4g(*^#%#"u8.ToArray() ),
                    ( long.MinValue, "234t(*&Q@%w34t"u8.ToArray() ),
                })
        };
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
                                  .BuildQueues();

        Assert.Single(actual);
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаЕстьОперацияУдаленияОчередиИзСнапшота__ДолженУдалитьОчередь()
    {
        var queueToDelete = QueueName.Parse("sample_queue");
        var leftQueue = new StubTaskQueue(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null,
            Array.Empty<(long, byte[])>());

        var snapshotQueues = new ITaskQueue[]
        {
            new StubTaskQueue(queueToDelete, PriorityQueueCode.Heap4Arity, null, null, null,
                new[] {( 1L, "aasdfasdf"u8.ToArray() ), ( 100L, "vasdaedhraerqa(Q#%V"u8.ToArray() )}),
            leftQueue,
        };

        var expected = new ITaskQueue[] {leftQueue,};

        var actual = StateRestorer.RestoreState(new QueueArraySnapshot(snapshotQueues),
                                   new Delta[] {new DeleteQueueDelta(queueToDelete),}
                                      .Select(x => x.Serialize()))
                                  .BuildQueues();

        Assert.Single(actual);
        Assert.Equal(expected, actual, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаЕстьСнапшотИЛогПустой__ДолженВернутьОчередиИзСнапшота()
    {
        var snapshotQueues = new ITaskQueue[]
        {
            new StubTaskQueue(QueueName.Parse("sample_queue"), PriorityQueueCode.Heap4Arity, null, null, null,
                new[] {( 1L, "aasdfasdf"u8.ToArray() ), ( 100L, "vasdaedhraerqa(Q#%V"u8.ToArray() )}),
            new StubTaskQueue(QueueName.Parse("orders:1002"), PriorityQueueCode.QueueArray, 100000, ( -10L, 10L ),
                null,
                new[]
                {
                    ( 9L, "hello, world"u8.ToArray() ), ( 9L, "hello, world"u8.ToArray() ),
                    ( 9L, "hello, world"u8.ToArray() ), ( 9L, "hello, world"u8.ToArray() ),
                }),
            new StubTaskQueue(QueueName.Parse("___@Q#%GWSA"), PriorityQueueCode.Heap4Arity, null, null, 123123,
                Array.Empty<(long, byte[])>())
        };

        var actual = StateRestorer.RestoreState(new QueueArraySnapshot(snapshotQueues), Array.Empty<byte[]>())
                                  .BuildQueues();

        Assert.Equal(snapshotQueues, actual, Comparer);
    }

    [Fact]
    public void RestoreState__КогдаЕстьОперацияСозданияНовойОчереди__ДолженСоздатьНовыеОчереди()
    {
        var queueToCreate = new StubTaskQueue(QueueName.Parse("aaaaaaaaaaa"), PriorityQueueCode.Heap4Arity,
            1000000, null, null, Array.Empty<(long, byte[])>());
        var expected = new ITaskQueue[] {queueToCreate,}.ToHashSet(Comparer);
        var snapshot = new QueueCollectionSnapshot(new QueueCollection());
        var actual = StateRestorer.RestoreState(snapshot,
                                   new Delta[]
                                   {
                                       new CreateQueueDelta(queueToCreate.Name, queueToCreate.Code,
                                           queueToCreate.Metadata.MaxQueueSize,
                                           queueToCreate.Metadata.MaxPayloadSize,
                                           queueToCreate.Metadata.PriorityRange)
                                   }.Select(d => d.Serialize()))
                                  .BuildQueues()
                                  .ToHashSet(Comparer);

        Assert.Equal(expected, actual, Comparer);
    }
}