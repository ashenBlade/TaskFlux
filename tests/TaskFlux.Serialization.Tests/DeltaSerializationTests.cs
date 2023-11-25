using TaskFlux.Models;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Serialization.Tests;

[Trait("Category", "Serialization")]
public class DeltaSerializationTests
{
    private static void AssertBase(Delta expected)
    {
        var data = expected.Serialize();
        var actual = Delta.DeserializeFrom(data);
        Assert.Equal(expected, actual, DeltaEqualityComparer.Instance);
    }

    public static IEnumerable<object[]> CreateQueueDeltaSerialization => new[]
    {
        new object[] {"", 0, 0, 0, null}, new object[] {"hello,world", 123, 123321, 12415, ( 14124L, 1423523452L )},
        new object[] {"orders:1:2023", 1, -1, -1, null},
        new object[] {"USERS_KNOWN________&", 10, 100, 1024 * 2, ( -10L, 10L )},
    };

    [Theory]
    [MemberData(nameof(CreateQueueDeltaSerialization))]
    public void CreateQueueDelta__Serialization(string queueName,
                                                int implementation,
                                                int maxQueueSize,
                                                int maxMessageSize,
                                                (long, long)? priorityRange)
    {
        AssertBase(new CreateQueueDelta(QueueNameParser.Parse(queueName), ( PriorityQueueCode ) implementation,
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
        AssertBase(new DeleteQueueDelta(QueueNameParser.Parse(queueName)));
    }

    [Theory]
    [InlineData("", 0L, new byte[] { })]
    [InlineData("", 123123L, new byte[] {1, 2, 3, 4, 5, 6, 7})]
    [InlineData("hello,world", long.MaxValue, new byte[] {255, 0, 1, 34, 66})]
    public void AddRecordDelta__Serialization(string queueName, long key, byte[] message)
    {
        AssertBase(new AddRecordDelta(QueueNameParser.Parse(queueName), key, message));
    }

    [Theory]
    [InlineData("", 0L, new byte[] { })]
    [InlineData("", 123123L, new byte[] {1, 2, 3, 4, 5, 6, 7})]
    [InlineData("hello,world", long.MaxValue, new byte[] {255, 0, 1, 34, 66})]
    public void RemoveRecordDelta__Serialization(string queueName, long key, byte[] message)
    {
        AssertBase(new RemoveRecordDelta(QueueNameParser.Parse(queueName), key, message));
    }
}