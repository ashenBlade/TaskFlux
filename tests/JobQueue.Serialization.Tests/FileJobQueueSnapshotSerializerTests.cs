using JobQueue.Core;

namespace JobQueue.Serialization.Tests;

[Trait("Category", "Serialization")]
public class FileJobQueueSnapshotSerializerTests
{
    public static readonly FileJobQueueSnapshotSerializer Serializer = new(new StubJobQueueFactory());

    private static void AssertBase(StubJobQueue queue)
    {
        var stream = new MemoryStream();
        Serializer.Serialize(stream, new[] {queue});
        stream.Position = 0;
        var actual = Serializer.Deserialize(stream).Single();
        Assert.Equal(queue, actual, JobQueueEqualityComparer.Instance);
    }

    private static readonly QueueName DefaultName = QueueNameParser.Parse("hello");

    private static IEnumerable<(long, byte[])> EmptyQueueData => Enumerable.Empty<(long, byte[])>();

    [Fact]
    public void Serialize__КогдаПереданаПустаяОчередьБезПредела()
    {
        AssertBase(new StubJobQueue(DefaultName, 0, EmptyQueueData));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(uint.MaxValue)]
    [InlineData(100)]
    [InlineData(128)]
    [InlineData(1000)]
    [InlineData(98765423)]
    public void Serialize__КогдаПереданаПустаяОчередьСПределом(uint limit)
    {
        AssertBase(new StubJobQueue(DefaultName, limit, EmptyQueueData));
    }

    [Theory]
    [InlineData(0, new byte[0])]
    [InlineData(1, new byte[] {123})]
    [InlineData(-1, new byte[] {byte.MaxValue})]
    [InlineData(long.MaxValue, new byte[] {byte.MaxValue, 0, 0, 0, 0, 0, 0, 0})]
    [InlineData(long.MinValue, new byte[] {5, 65, 22, 75, 97, 32, 200})]
    [InlineData(123123, new byte[] {byte.MaxValue, 1, 2, 3, 4, 5, 6, byte.MinValue})]
    public void Serialize__КогдаПереданаОчередьС1ЭлементомБезПредела(long priority, byte[] data)
    {
        AssertBase(new StubJobQueue(DefaultName, 0, new[] {( priority, data )}));
    }

    private static readonly Random Random = new Random(87);

    private IEnumerable<(long, byte[])> CreateRandomQueueElements(int count)
    {
        for (int i = 0; i < count; i++)
        {
            var buffer = new byte[Random.Next(0, 100)];
            Random.NextBytes(buffer);
            yield return ( Random.NextInt64(), buffer );
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    [InlineData(20)]
    [InlineData(100)]
    [InlineData(128)]
    public void Serialize__КогдаЭлементовНесколько(int count)
    {
        AssertBase(new StubJobQueue(DefaultName, 0, CreateRandomQueueElements(count)));
    }
}