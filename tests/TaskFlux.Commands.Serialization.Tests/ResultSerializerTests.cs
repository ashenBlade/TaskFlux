using JobQueue.Core;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using Xunit;

namespace TaskFlux.Commands.Serialization.Tests;

// ReSharper disable StringLiteralTypo
[Trait("Category", "Serialization")]
public class ResultSerializerTests
{
    private static readonly ResultSerializer Serializer = new();

    private static void AssertBase(Result expected)
    {
        var serialized = Serializer.Serialize(expected);
        var actual = Serializer.Deserialize(serialized);
        Assert.Equal(expected, actual, ResultEqualityComparer.Instance);
    }

    [Theory(DisplayName = nameof(CountResult))]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(123)]
    [InlineData(32)]
    [InlineData(uint.MaxValue)]
    [InlineData(uint.MaxValue - 1)]
    [InlineData(10)]
    [InlineData(uint.MaxValue / 2)]
    [InlineData(1 << 10)]
    [InlineData(1 << 2)]
    public void CountResult__Serialization(uint result)
    {
        AssertBase(new CountResult(result));
    }

    [Theory(DisplayName = nameof(EnqueueResult))]
    [InlineData(true)]
    [InlineData(false)]
    public void EnqueueResult__Serialization(bool success)
    {
        AssertBase(new EnqueueResult(success));
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

    [Theory(DisplayName = nameof(DequeueResult))]
    [MemberData(nameof(KeyPayloadSize))]
    public void DequeueResult__Success__Serialization(int key, int payloadSize)
    {
        var buffer = new byte[payloadSize];
        Random.Shared.NextBytes(buffer);
        AssertBase(DequeueResult.Create(key, buffer));
    }

    [Theory(DisplayName = nameof(ErrorResult))]
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
        AssertBase(new ErrorResult(errorType, message));
    }

    [Fact(DisplayName = nameof(OkResult))]
    public void OkResult__Serialization()
    {
        AssertBase(new OkResult());
    }

    private class StubMetadata : IJobQueueMetadata
    {
        public StubMetadata(QueueName queueName, uint maxSize, uint count)
        {
            QueueName = queueName;
            MaxSize = maxSize;
            Count = count;
        }

        public QueueName QueueName { get; }
        public uint Count { get; }
        public uint MaxSize { get; }
    }

    [Theory(DisplayName = $"{nameof(ListQueuesResult)}-Single")]
    [InlineData("", 0, 0)]
    [InlineData("", 111, 123)]
    [InlineData("queue", 0, 0)]
    [InlineData("SDGGGGGGGJjsdhU&^%*Ahvc`2eu84t((AFP\"vawergf'", 100, 100)]
    [InlineData(
        "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~",
        uint.MaxValue, 0)]
    [InlineData("-", uint.MaxValue, uint.MaxValue)]
    [InlineData("default", uint.MaxValue - 2, uint.MaxValue - 1)]
    [InlineData("hello,world!", uint.MaxValue - 2, uint.MaxValue - 1)]
    public void ListQueuesResult__SingleQueue__Serialization(string queueName, uint count, uint maxSize)
    {
        var metadata = new StubMetadata(QueueNameParser.Parse(queueName), maxSize, count);
        AssertBase(new ListQueuesResult(new[] {metadata}));
    }

    public static IEnumerable<object[]> ListQueuesArguments => new[]
    {
        new object[] {new (string, uint, uint)[] {( "", 123, 123 ), ( "default", 0, 0 ),},},
        new object[]
        {
            new (string, uint, uint)[]
            {
                ( "", 123, 123 ), ( "default", 0, 0 ), ( "queue:test:1", 123, 0 ),
                ( "hello,world!", uint.MaxValue, 0 )
            },
        },
        new object[]
        {
            new (string, uint, uint)[]
            {
                ( "______", 0, int.MaxValue ), ( "default", 0, 1232323 ), ( "!!!!!", 123, 3434343434 ),
                ( "123", uint.MaxValue, 0 ), ( "[[[[[[]]]]]]]", 1, 1 ), ( "UwU", 123123, 999999 ),
            },
        },
        new object[]
        {
            new (string, uint, uint)[] {( "", 123, 123 ), ( "default", 0, 0 ), ( "```````", uint.MaxValue, 0 )},
        },
        new object[]
        {
            new (string, uint, uint)[]
            {
                ( "!", 123, 123 ), ( "default", 0, 0 ), ( ":", 123, 0 ), ( "~", uint.MaxValue, 0 ),
                ( "~!", uint.MaxValue, 0 ), ( "~!!", uint.MaxValue, 0 ),
            },
        },
    };

    [Theory(DisplayName = $"{nameof(ListQueuesResult)}-MultipleItems")]
    [MemberData(nameof(ListQueuesArguments))]
    public void ListQueuesResult__MultipleQueues__Serialization((string Name, uint Count, uint MaxSize)[] values)
    {
        var metadata = values.Select(v => new StubMetadata(QueueNameParser.Parse(v.Name), v.MaxSize, v.Count))
                             .ToList();
        AssertBase(new ListQueuesResult(metadata));
    }

    [Fact(DisplayName = $"{nameof(ListQueuesResult)}-Empty")]
    public void ListQueuesResult__Empty__Serialization()
    {
        AssertBase(new ListQueuesResult(Array.Empty<IJobQueueMetadata>()));
    }
}