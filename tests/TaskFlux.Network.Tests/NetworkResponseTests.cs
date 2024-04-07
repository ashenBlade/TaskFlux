using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Network.Responses;
using TaskFlux.Network.Responses.Policies;

namespace TaskFlux.Network.Tests;

[Trait("Category", "Serialization")]
public class NetworkResponseTests
{
    private static void AssertBase(NetworkResponse expected)
    {
        var stream = new MemoryStream();
        expected.SerializeAsync(stream, CancellationToken.None).GetAwaiter().GetResult();
        stream.Position = 0;
        var actual = NetworkResponse.DeserializeAsync(stream, CancellationToken.None).GetAwaiter().GetResult();
        Assert.Equal(expected, actual, NetworkResponseEqualityComparer.Instance);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(1000)]
    [InlineData(int.MaxValue)]
    public void Count__Serialization(int count)
    {
        AssertBase(new CountNetworkResponse(count));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void Dequeue__Priority__Serialization(long priority)
    {
        var id = new RecordId(123);
        var data = "данные в сообщении"u8.ToArray();
        AssertBase(new DequeueNetworkResponse(new QueueRecord(id, priority, data)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(123)]
    [InlineData(578645)]
    [InlineData(ulong.MaxValue)]
    public void Dequeue__Id__Serialization(ulong id)
    {
        AssertBase(new DequeueNetworkResponse(new QueueRecord(new RecordId(id), 0, "asdfasdfasdf"u8.ToArray())));
    }

    public static IEnumerable<object[]> DequeuePayload => new object[][]
    {
        [Array.Empty<byte>()], [new byte[] {1}], [new[] {byte.MaxValue}], [new[] {byte.MinValue}],
        [new byte[] {1, 2, 3, 4, 5, 6}], [Enumerable.Range(0, 1024).Select(i => ( byte ) ( i % 256 )).ToArray()],
    };

    [Theory]
    [MemberData(nameof(DequeuePayload))]
    public void Dequeue__Payload__Serialization(byte[] payload)
    {
        const long priority = 1111;
        AssertBase(new DequeueNetworkResponse(new QueueRecord(new RecordId(1), priority, payload)));
    }

    [Fact]
    public void Dequeue__Null__Serialization()
    {
        AssertBase(new DequeueNetworkResponse(null));
    }

    [Fact]
    public void Error__Type__Serialization()
    {
        const string message = "sample message";
        for (int i = 0; i <= byte.MaxValue; i++)
        {
            AssertBase(new ErrorNetworkResponse(( byte ) i, message));
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("sample message")]
    [InlineData("сообщение об ошибке")]
    [InlineData("Ошибка при выполнении")]
    [InlineData("SDF^)_(W*B\0OWSPGWB{W   {__+A)IV()_)(*^&%R@ CAA\n\tt")]
    public void Error__Message__Serialization(string message)
    {
        AssertBase(new ErrorNetworkResponse(1, message));
    }

    public static IEnumerable<object[]> TaskQueueInfos => new[]
    {
        new object[] {Array.Empty<ITaskQueueInfo>()}, new object[] {new StubTaskQueueInfo[] {new("queue", 0),}},
        new object[]
        {
            new StubTaskQueueInfo[]
            {
                new("queue", 0) {Policies = {["sample"] = "hello"}},
                new("another_queue", 1) {Policies = {["sample"] = "hello", ["hi"] = "hola!  "}},
            }
        },
        new object[]
        {
            Enumerable.Range(0, 10)
                      .Select(i => new StubTaskQueueInfo(i.ToString(), i)
                       {
                           Policies = {[i.ToString()] = i.ToString(), [( i * 100 ).ToString()] = ( -i ).ToString()}
                       })
                      .ToArray()
        }
    };

    [Theory]
    [MemberData(nameof(TaskQueueInfos))]
    public void ListQueues__Serialization(ITaskQueueInfo[] queues)
    {
        AssertBase(new ListQueuesNetworkResponse(queues));
    }

    private class StubTaskQueueInfo : ITaskQueueInfo
    {
        public StubTaskQueueInfo(string name, int count)
        {
            QueueName = QueueName.Parse(name);
            Count = count;
        }

        public QueueName QueueName { get; }
        public int Count { get; }
        public Dictionary<string, string> Policies { get; } = new();
    }

    public static IEnumerable<object[]> NetworkPolicies => new[]
    {
        new NetworkQueuePolicy[] {new GenericNetworkQueuePolicy("")},
        new NetworkQueuePolicy[] {new GenericNetworkQueuePolicy("сообщение об ошибке")},
        new NetworkQueuePolicy[] {new GenericNetworkQueuePolicy("error message\t\t\n\\")},
        new NetworkQueuePolicy[] {new MaxQueueSizeNetworkQueuePolicy(0)},
        new NetworkQueuePolicy[] {new MaxQueueSizeNetworkQueuePolicy(1)},
        new NetworkQueuePolicy[] {new MaxQueueSizeNetworkQueuePolicy(10)},
        new NetworkQueuePolicy[] {new MaxQueueSizeNetworkQueuePolicy(int.MaxValue)},
        new NetworkQueuePolicy[] {new MaxMessageSizeNetworkQueuePolicy(0)},
        new NetworkQueuePolicy[] {new MaxMessageSizeNetworkQueuePolicy(1)},
        new NetworkQueuePolicy[] {new MaxMessageSizeNetworkQueuePolicy(1024)},
        new NetworkQueuePolicy[] {new MaxMessageSizeNetworkQueuePolicy(int.MaxValue)},
        new NetworkQueuePolicy[] {new PriorityRangeNetworkQueuePolicy(0, 0)},
        new NetworkQueuePolicy[] {new PriorityRangeNetworkQueuePolicy(0, long.MaxValue)},
        new NetworkQueuePolicy[] {new PriorityRangeNetworkQueuePolicy(0, 1)},
        new NetworkQueuePolicy[] {new PriorityRangeNetworkQueuePolicy(-1, 1)},
        new NetworkQueuePolicy[] {new PriorityRangeNetworkQueuePolicy(long.MinValue, long.MaxValue)},
        new NetworkQueuePolicy[] {new PriorityRangeNetworkQueuePolicy(long.MinValue, -1)},
    };

    [Theory]
    [MemberData(nameof(NetworkPolicies))]
    public void PolicyViolation__Serialization(NetworkQueuePolicy queuePolicy)
    {
        AssertBase(new PolicyViolationNetworkResponse(queuePolicy));
    }
}