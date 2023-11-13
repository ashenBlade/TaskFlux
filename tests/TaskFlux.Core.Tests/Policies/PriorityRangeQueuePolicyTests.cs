using FluentAssertions;
using Moq;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Queue;
using Xunit;

namespace TaskFlux.Core.Tests.Policies;

[Trait("Category", "BusinessLogic")]
public class PriorityRangeQueuePolicyTests
{
    private static readonly IReadOnlyTaskQueue Queue = new Mock<IReadOnlyTaskQueue>(MockBehavior.Strict).Object;
    private static readonly byte[] Payload = Array.Empty<byte>();

    [Theory]
    [InlineData(-1L, 0, 1L)]
    [InlineData(-10L, 0, 10L)]
    [InlineData(0L, 4L, 10L)]
    [InlineData(-10L, -4L, 0L)]
    [InlineData(0L, 100L, long.MaxValue)]
    [InlineData(long.MinValue, 10000000L, long.MaxValue)]
    public void CanEnqueue__КогдаПриоритетВходитВИнтервал__ДолженВернутьTrue(long min, long priority, long max)
    {
        var policy = new PriorityRangeQueuePolicy(min, max);

        var canEnqueue = policy.CanEnqueue(priority, Payload, Queue);

        canEnqueue
           .Should()
           .BeTrue("Приоритет находится в устновленном интервале");
    }

    [Theory]
    [InlineData(0L, 0L)]
    [InlineData(1L, 2L)]
    [InlineData(0L, 4L)]
    [InlineData(long.MinValue, long.MaxValue)]
    [InlineData(10000L, 10010L)]
    public void CanEnqueue__КогдаПриоритетРавенГраничнымЗначениям__ДолженВернутьTrue(long min, long max)
    {
        var policy = new PriorityRangeQueuePolicy(min, max);

        var canEnqueueMin = policy.CanEnqueue(min, Payload, Queue);

        canEnqueueMin
           .Should()
           .BeTrue("Минимальная граница должна учитываться включительно");

        var canEnqueueMax = policy.CanEnqueue(max, Payload, Queue);

        canEnqueueMax
           .Should()
           .BeTrue("Максимальная граница должна учитывать включительно");
    }

    [Theory]
    [InlineData(-10L, 11L, 10L)]
    [InlineData(-10L, -11L, 10L)]
    [InlineData(0, -1L, long.MaxValue)]
    [InlineData(long.MinValue, 1L, 0L)]
    [InlineData(-1L, 5L, 4L)]
    public void CanEnqueue__КогдаПриоритетВыходитЗаГраницыДиапазона__ДолженВернутьFalse(
        long min,
        long priority,
        long max)
    {
        var policy = new PriorityRangeQueuePolicy(min, max);

        var canEnqueue = policy.CanEnqueue(priority, Payload, Queue);

        canEnqueue
           .Should()
           .BeFalse("Приоритет {0} выходит из интервала {1} : {2}", priority, min, max);
    }
}