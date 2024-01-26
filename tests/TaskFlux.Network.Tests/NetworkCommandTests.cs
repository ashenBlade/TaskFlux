using TaskFlux.Core;
using TaskFlux.Network.Commands;

namespace TaskFlux.Network.Tests;

[Trait("Category", "Serialization")]
public class NetworkCommandTests
{
    private static void AssertBase(NetworkCommand command)
    {
        var stream = new MemoryStream();
        command.SerializeAsync(stream, CancellationToken.None).GetAwaiter().GetResult();
        stream.Position = 0;
        var actual = NetworkCommand.DeserializeAsync(stream, CancellationToken.None).GetAwaiter().GetResult();
        Assert.Equal(command, actual, NetworkCommandEqualityComparer.Instance);
    }

    [Theory]
    [InlineData("")]
    [InlineData("sample")]
    [InlineData("queue-name:22")]
    public void Count__Serialization(string queue)
    {
        AssertBase(new CountNetworkCommand(queue));
    }

    [Theory]
    [InlineData("")]
    [InlineData("sample")]
    [InlineData("queue-name:22")]
    public void CreateQueue__QueueName__Serialization(string name)
    {
        AssertBase(new CreateQueueNetworkCommand(QueueName.Parse(name), 1, null, null, null));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(100)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(int.MaxValue)]
    public void CreateQueue__Code__Serialization(int code)
    {
        AssertBase(new CreateQueueNetworkCommand(QueueName.Default, code, null, null, null));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(512)]
    [InlineData(1024)]
    [InlineData(int.MaxValue)]
    public void CreateQueue__MaxQueueSize__Serialization(int maxSize)
    {
        AssertBase(new CreateQueueNetworkCommand(QueueName.Default, 1, maxSize, null, null));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(512)]
    [InlineData(1024)]
    [InlineData(int.MaxValue)]
    public void CreateQueue__MaxMessageSize__Serialization(int maxSize)
    {
        AssertBase(new CreateQueueNetworkCommand(QueueName.Default, 1, null, maxSize, null));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(0, 10)]
    [InlineData(-1, 1)]
    [InlineData(long.MinValue, 0)]
    [InlineData(0, long.MaxValue)]
    public void CreateQueue__PriorityRange__Serialization(long min, long max)
    {
        AssertBase(new CreateQueueNetworkCommand(QueueName.Default, 1, null, null,
            ( min, max )));
    }

    [Theory]
    [InlineData("")]
    [InlineData("sample-queue")]
    [InlineData("spam:1:123")]
    public void DeleteQueue__Serialization(string queue)
    {
        AssertBase(new DeleteQueueNetworkCommand(QueueName.Parse(queue)));
    }

    [Theory]
    [InlineData("")]
    [InlineData("sample-queue")]
    [InlineData("spam:1:123")]
    public void Dequeue__Serialization(string queue)
    {
        AssertBase(new DequeueNetworkCommand(QueueName.Parse(queue)));
    }

    [Theory]
    [InlineData("")]
    [InlineData("sample-queue")]
    [InlineData("spam:1:123")]
    public void Enqueue__QueueName__Serialization(string queue)
    {
        AssertBase(new EnqueueNetworkCommand(QueueName.Parse(queue), 1, new byte[] {0}));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(1024)]
    [InlineData(-1)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void Enqueue__Key__Serialization(long key)
    {
        AssertBase(new EnqueueNetworkCommand(QueueName.Default, key, new byte[] {0}));
    }

    [Theory]
    [InlineData(new byte[0])]
    [InlineData(new byte[] {1, 2, 3})]
    [InlineData(new byte[] {0})]
    [InlineData(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14})]
    public void Enqueue__Message__Serialization(byte[] message)
    {
        AssertBase(new EnqueueNetworkCommand(QueueName.Default, 0, message));
    }

    [Fact]
    public void ListQueues__Serialization()
    {
        AssertBase(new ListQueuesNetworkCommand());
    }
}