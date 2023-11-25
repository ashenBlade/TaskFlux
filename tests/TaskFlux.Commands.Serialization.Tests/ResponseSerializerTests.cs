using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.PolicyViolation;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Queue;
using TaskFlux.Models;
using TaskFlux.PriorityQueue;
using Xunit;

namespace TaskFlux.Commands.Serialization.Tests;

// ReSharper disable StringLiteralTypo
[Trait("Category", "Serialization")]
public class ResponseSerializerTests
{
    private static readonly ResponseSerializer Serializer = new();

    private static void AssertBase(Response expected)
    {
        var serialized = Serializer.Serialize(expected);
        var actual = Serializer.Deserialize(serialized);
        Assert.Equal(expected, actual, ResponseEqualityComparer.Instance);
    }

    [Theory(DisplayName = nameof(CountResponse))]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(123)]
    [InlineData(32)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(10)]
    [InlineData(int.MaxValue / 2)]
    [InlineData(1 << 10)]
    [InlineData(1 << 2)]
    public void CountResult__Serialization(int result)
    {
        AssertBase(new CountResponse(result));
    }

    public static IEnumerable<object[]> KeyPayloadSize => CreateKeyPayload();

    private static IEnumerable<object[]> CreateKeyPayload()
    {
        var keys = new[] {-1, 0, 1, 2, int.MaxValue, int.MinValue, 100, byte.MaxValue, short.MaxValue, 127};
        var payloadSizes = new[] {0, 1, 2, 3, 10, 20, byte.MaxValue};
        foreach (var key in keys)
        {
            foreach (var size in payloadSizes)
            {
                yield return new object[] {key, size};
            }
        }
    }

    [Theory(DisplayName = nameof(DequeueResponse))]
    [MemberData(nameof(KeyPayloadSize))]
    public void DequeueResult__Success__Serialization(int key, int payloadSize)
    {
        var buffer = new byte[payloadSize];
        Random.Shared.NextBytes(buffer);
        AssertBase(DequeueResponse.Create(key, buffer));
    }

    [Theory(DisplayName = nameof(ErrorResponse))]
    [InlineData(ErrorType.Unknown, "")]
    [InlineData(ErrorType.Unknown, "Some message error")]
    [InlineData(ErrorType.Unknown, "Странный ключ??!.")]
    [InlineData(ErrorType.InvalidQueueName, "Название очереди слишком длинное")]
    [InlineData(ErrorType.InvalidQueueName, "В названии очереди недопустимые символы")]
    [InlineData(ErrorType.QueueDoesNotExist, "")]
    [InlineData(ErrorType.QueueDoesNotExist, "Queue with specified name does not exist")]
    [InlineData(ErrorType.QueueDoesNotExist, "Такой очереди не существует")]
    public void ErrorResult__Serialization(ErrorType errorType, string message)
    {
        AssertBase(new ErrorResponse(errorType, message));
    }

    [Fact(DisplayName = nameof(OkResponse))]
    public void OkResult__Serialization()
    {
        AssertBase(new OkResponse());
    }

    private class StubMetadata : ITaskQueueMetadata
    {
        public StubMetadata(QueueName queueName,
                            int? maxSize,
                            int count,
                            int? maxPayloadSize,
                            (long, long)? priorityRange,
                            PriorityQueueCode code)
        {
            QueueName = queueName;
            MaxSize = maxSize;
            Count = count;
            MaxPayloadSize = maxPayloadSize;
            PriorityRange = priorityRange;
            Code = code;
        }

        public QueueName QueueName { get; }
        public PriorityQueueCode Code { get; }
        public int Count { get; }
        public int? MaxSize { get; }
        public int? MaxPayloadSize { get; }
        public (long Min, long Max)? PriorityRange { get; }
    }

    [Theory(DisplayName = $"{nameof(ListQueuesResponse)}-Single")]
    [InlineData("", 0, 0, 123, 0L, 1000L)]
    [InlineData("", 111, 123, 0, long.MinValue, long.MaxValue)]
    [InlineData("queue", 0, 0, int.MaxValue, -123123, 12314122)]
    [InlineData("SDGGGGGGGJjsdhU&^%*Ahvc`2eu84t((AFP\"vawergf'", 100, 100, 123412, 0, 0)]
    [InlineData(
        "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~",
        int.MaxValue, 0, 1342543, 1L, long.MaxValue)]
    [InlineData("-", int.MaxValue, int.MaxValue, int.MaxValue, long.MaxValue - 2, long.MaxValue - 1)]
    [InlineData("default", int.MaxValue - 2, int.MaxValue - 1, 11111111, 1L << 63, ( 1L << 63 ) + 1)]
    [InlineData("hello,world!", int.MaxValue - 2, int.MaxValue - 1, 0, 0, 0)]
    public void ListQueuesResult__SingleQueue__Serialization(string queueName,
                                                             int count,
                                                             int maxSize,
                                                             int maxPayloadSize,
                                                             long min,
                                                             long max)
    {
        var metadata = new StubMetadata(QueueNameParser.Parse(queueName), maxSize, count, maxPayloadSize, ( min, max ),
            PriorityQueueCode.Heap4Arity);
        AssertBase(new ListQueuesResponse(new[] {metadata}));
    }

    public static IEnumerable<object[]> ListQueuesArguments => new[]
    {
        new object[]
        {
            new (string, int, int?, int?, (long, long)?)[]
            {
                ( "", 123, null, null, null ), ( "default", 0, 0, 1, ( 0L, long.MaxValue ) ),
            },
        },
        new object[]
        {
            new (string, int, int?, int?, (long, long)?)[]
            {
                ( "", 123, null, int.MaxValue, ( long.MinValue, long.MaxValue ) ),
                ( "default", 0, null, 0, ( -1L, -1L ) ),
                ( "queue:test:1", 123, null, null, ( -1000L, 1000L ) ),
                ( "hello,world!", int.MaxValue, 0, 1024 * 1024, ( 123123123L, 123123124L ) )
            },
        },
        new object[]
        {
            new (string, int, int?, int?, (long, long)?)[]
            {
                ( "______", 0, int.MaxValue, 1024 * 1024 * 2, ( 0L, 3L ) ),
                ( "default", 0, 1232323, 9090 * 123, ( -1000L, 3333L ) ),
                ( "!!!!!", 123, null, null, ( 9L, 23L ) ),
                ( "123", int.MaxValue, 0, null, ( 1000L, 50000L ) ),
                ( "[[[[[[]]]]]]]", 1, 1, ( int ) ( 1024 * 1024 * 1.4 ), null ),
                ( "UwU", 123123, 999999, 1024, ( -1L, 10L ) ),
            },
        },
        new object[]
        {
            new (string, int, int?, int?, (long, long)?)[]
            {
                ( "", 123, 123, null, ( 1L, 1L ) ), ( "default", 0, 0, 20480, null ),
                ( "```````", int.MaxValue, 0, 7890, ( 123L, 125L ) )
            },
        },
        new object[]
        {
            new (string, int, int?, int?, (long, long)?)[]
            {
                ( "!", 123, 123, 1, ( long.MinValue, 0L ) ), ( "default", 0, 0, null, null ),
                ( ":", 123, 0, int.MaxValue / 2, ( 0L, long.MaxValue ) ),
                ( "~", int.MaxValue, 0, 20 * 1024 * 1024, null ),
                ( "~!", int.MaxValue, null, 512 + 256 + 128, ( long.MinValue / 16, long.MaxValue / 16 ) ),
                ( "~!!", int.MaxValue, 0, null, ( 1L, 5L ) ),
            },
        },
    };

    [Theory(DisplayName = $"{nameof(ListQueuesResponse)}-MultipleItems")]
    [MemberData(nameof(ListQueuesArguments))]
    public void ListQueuesResult__MultipleQueues__Serialization(
        (string Name, int Count, int? MaxSize, int? MaxPayloadSize, (long, long)? PriorityRange)[] values)
    {
        var metadata = values
                      .Select(v => new StubMetadata(QueueNameParser.Parse(v.Name), v.MaxSize, v.Count, v.MaxPayloadSize,
                           v.PriorityRange, PriorityQueueCode.QueueArray))
                      .ToList();
        AssertBase(new ListQueuesResponse(metadata));
    }

    [Fact(DisplayName = $"{nameof(ListQueuesResponse)}-Empty")]
    public void ListQueuesResult__Empty__Serialization()
    {
        AssertBase(new ListQueuesResponse(Array.Empty<ITaskQueueMetadata>()));
    }

    [Theory(DisplayName = $"{nameof(PolicyViolationResponse)}-MaxQueueSize")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(1000)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(int.MaxValue)]
    public void PolicyViolationResult__MaxQueueSize__Serialization(int maxQueueSize)
    {
        AssertBase(new PolicyViolationResponse(new MaxQueueSizeQueuePolicy(maxQueueSize)));
    }

    [Theory(DisplayName = $"{nameof(PolicyViolationResponse)}-MaxPayloadSize")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(1024)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(int.MaxValue)]
    public void PolicyViolationResponse__MaxPayloadSize__Serialization(int maxPayloadSize)
    {
        AssertBase(new PolicyViolationResponse(new MaxPayloadSizeQueuePolicy(maxPayloadSize)));
    }

    [Theory(DisplayName = $"{nameof(PolicyViolationResponse)}-PriorityRange")]
    [InlineData(0L, 0L)]
    [InlineData(long.MinValue, long.MaxValue)]
    [InlineData(0L, long.MaxValue)]
    [InlineData(-10L, 100L)]
    public void PolicyViolationResponse__PriorityRange__Serialization(long min, long max)
    {
        AssertBase(new PolicyViolationResponse(new PriorityRangeQueuePolicy(min, max)));
    }
}