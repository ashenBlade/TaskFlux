using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using Xunit;

namespace TaskFlux.Commands.Serialization.Tests;

public class CommandSerializerTests
{
    private static readonly CommandSerializer Serializer = new();

    private static void AssertBase(Command command)
    {
        var serialized = Serializer.Serialize(command);
        var actual = Serializer.Deserialize(serialized);
        Assert.Equal(command, actual, CommandEqualityComparer.Instance);
    }

    [Fact]
    public void CountCommand__Serialization()
    {
        AssertBase(new CountCommand());
    }

    [Fact]
    public void DequeueCommand__Serialization()
    {
        AssertBase(new DequeueCommand());
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(1, 1)]
    [InlineData(1, 10)]
    [InlineData(0, 1)]
    [InlineData(0, 0)]
    [InlineData(0, 2)]
    [InlineData(0, 123)]
    [InlineData(123, 100)]
    [InlineData(int.MaxValue, 0)]
    [InlineData(int.MinValue, 0)]
    [InlineData(int.MinValue, 1)]
    [InlineData(int.MaxValue, 1)]
    [InlineData(int.MaxValue, 100)]
    [InlineData(-1, byte.MaxValue)]
    public void EnqueueCommand__Serialization(int key, int payloadLength)
    {
        var buffer = new byte[payloadLength];
        Random.Shared.NextBytes(buffer);
        AssertBase(new EnqueueCommand(key, buffer));
    }
}