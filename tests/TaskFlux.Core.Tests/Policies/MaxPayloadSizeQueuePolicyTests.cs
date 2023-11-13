using FluentAssertions;
using Moq;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Queue;
using Xunit;

namespace TaskFlux.Core.Tests.Policies;

[Trait("Category", "BusinessLogic")]
public class MaxPayloadSizeQueuePolicyTests
{
    private static readonly IReadOnlyTaskQueue Queue = new Mock<IReadOnlyTaskQueue>(MockBehavior.Strict).Object;

    private static IReadOnlyList<byte> CreatePayloadMock(int count) =>
        Mock.Of<IReadOnlyList<byte>>(x => x.Count == count);

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 100)]
    [InlineData(1023, 1024)]
    [InlineData(1024 * 2, 1024 * 4)]
    public void CanEnqueue__КогдаРазмерТелаМеньшеМаксимального__ДолженВернутьTrue(int payloadSize, int maxPayloadSize)
    {
        var policy = new MaxPayloadSizeQueuePolicy(maxPayloadSize);
        var payload = CreatePayloadMock(payloadSize);

        var canEnqueue = policy.CanEnqueue(0, payload, Queue);

        canEnqueue
           .Should()
           .BeTrue("Добавить можно так как размер тела меньше максимального");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(1024)]
    [InlineData(int.MaxValue)]
    public void CanEnqueue__КогдаРазмерТелаРавенМаксимальному__ДолженВернутьTrue(int maxPayloadSize)
    {
        var policy = new MaxPayloadSizeQueuePolicy(maxPayloadSize);
        var payload = CreatePayloadMock(maxPayloadSize);

        var canEnqueue = policy.CanEnqueue(0, payload, Queue);

        canEnqueue
           .Should()
           .BeTrue("Размер тела хоть и равен максимальному, но не превышен");
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(16 * 1024, 100)]
    [InlineData(1024 * 2, 1024)]
    [InlineData(1025, 1024)]
    [InlineData(int.MaxValue, 1024)]
    public void CanEnqueue__КогдаРазмерТелаБольшеМаксимального__ДолженВернутьFalse(int payloadSize, int maxPayloadSize)
    {
        var policy = new MaxPayloadSizeQueuePolicy(maxPayloadSize);
        var payload = CreatePayloadMock(payloadSize);

        var canEnqueue = policy.CanEnqueue(0, payload, Queue);

        canEnqueue
           .Should()
           .BeFalse("Размер тела превысил максимальный");
    }
}