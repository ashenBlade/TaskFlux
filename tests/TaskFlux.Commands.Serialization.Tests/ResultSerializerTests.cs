using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using Xunit;

namespace TaskFlux.Commands.Serialization.Tests;

public class ResultSerializerTests
{
    public static readonly ResultSerializer Serializer = new();

    public void AssertBase(Result expected)
    {
        var serialized = Serializer.Serialize(expected);
        var actual = Serializer.Deserialize(serialized);
        Assert.Equal(expected, actual, ResultEqualityComparer.Instance);
    }

    [Theory]
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
        AssertBase(new CountResult(result));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void EnqueueResult__Serialization(bool success)
    {
        AssertBase(new EnqueueResult(success));
    }

    public static IEnumerable<object[]> KeyPayloadSize => CreateKeyPayload();

    private static IEnumerable<object[]> CreateKeyPayload()
    {
        var keys = new[] {-1, 0, 1, 2, int.MaxValue, int.MinValue, 100, byte.MaxValue, short.MaxValue, 127 };
        var payloadSizes = new[] {0, 1, 2, 3, 10, 20, byte.MaxValue};
        foreach (var key in keys)
        {
            foreach (var size in payloadSizes)
            {
                yield return new object[] {key, size};
            }
        }
    }

    [Theory]
    [MemberData(nameof(KeyPayloadSize))]
    public void DequeueResult__Success__Serialization(int key, int payloadSize)
    {
        var buffer = new byte[payloadSize];
        Random.Shared.NextBytes(buffer);
        AssertBase(DequeueResult.Create(key, buffer));    
    }
}