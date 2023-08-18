using JobQueue.Core;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using Xunit;

namespace TaskFlux.Commands.Serialization.Tests;

[Trait("Category", "Serialization")]
public class CommandSerializerTests
{
    private static readonly CommandSerializer Serializer = new();

    private static void AssertBase(Command command)
    {
        var serialized = Serializer.Serialize(command);
        var actual = Serializer.Deserialize(serialized);
        Assert.Equal(command, actual, CommandEqualityComparer.Instance);
    }

    [Theory]
    [InlineData("")]
    [InlineData("default")]
    [InlineData("queue")]
    [InlineData("adfdfff")]
    [InlineData("what??")]
    [InlineData("queue.number.uno1")]
    [InlineData("queue.number.uno2")]
    [InlineData("hello-world")]
    [InlineData("hello-world.com")]
    [InlineData("queue:2:help")]
    [InlineData("default.1.oops")]
    [InlineData("main.DEV_OPS")]
    public void CountCommand__Serialization(string queueName)
    {
        AssertBase(new CountCommand(QueueName.Parse(queueName)));
    }

    [Theory]
    [InlineData("")]
    [InlineData("default")]
    [InlineData("sample-queue")]
    [InlineData("xxx")]
    [InlineData("nice.nice.uwu")]
    [InlineData("hello,world")]
    public void DequeueCommand__Serialization(string queueName)
    {
        AssertBase(new DequeueCommand(QueueName.Parse(queueName)));
    }

    [Theory]
    [InlineData(1, 0, "")]
    [InlineData(1, 1, "default")]
    [InlineData(1, 10, "what??")]
    [InlineData(0, 1, "queueNumberOne")]
    [InlineData(0, 0, "asdf")]
    [InlineData(0, 2, "nice.uwu")]
    [InlineData(0, 123, "supra222")]
    [InlineData(123, 100, "queue")]
    [InlineData(int.MaxValue, 0, "default.queue")]
    [InlineData(int.MaxValue, byte.MaxValue, "what.is.it")]
    [InlineData(-1, 1, "abc-sss.u343e")]
    [InlineData(long.MaxValue, 0, "queue")]
    [InlineData(long.MinValue, 0, "")]
    [InlineData(long.MinValue, 1, "nope")]
    [InlineData(long.MaxValue, 1, "uiii")]
    [InlineData(long.MaxValue, 100, "q123oeire")]
    [InlineData((long) int.MaxValue + 1, 100, "!dfd...dsf")]
    [InlineData(long.MaxValue - 1, 2, "asdfv")]
    [InlineData(-1, byte.MaxValue, "dfdq135f")]
    public void EnqueueCommand__Serialization(long key, int payloadLength, string queueName)
    {
        var buffer = new byte[payloadLength];
        Random.Shared.NextBytes(buffer);
        AssertBase(new EnqueueCommand(key, buffer, QueueName.Parse(queueName)));
    }
}