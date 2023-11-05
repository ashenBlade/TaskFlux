using FluentAssertions;
using Moq;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Queue;
using Xunit;

namespace TaskFlux.Core.Tests.Policies;

[Trait("Category", "BusinessLogic")]
public class MaxQueueSizeQueuePolicyTests
{
    [Theory]
    [InlineData(0, 100)]
    [InlineData(10, 20)]
    [InlineData(100, 1000)]
    [InlineData(128, int.MaxValue)]
    [InlineData(int.MaxValue - 1, int.MaxValue)]
    public void CanEnqueue__КогдаРазмерМеньшеУказанного__ДолженРазрешитьДобавление(int currentSize, int maxQueueSize)
    {
        var policy = new MaxQueueSizeQueuePolicy(maxQueueSize);
        var queue = Mock.Of<IReadOnlyTaskQueue>(q => q.Count == currentSize);

        var canEnqueue = policy.CanEnqueue(0, Array.Empty<byte>(), queue);

        canEnqueue
           .Should()
           .BeTrue("Текущий размер {0} меньше максимального {1}", currentSize, maxQueueSize);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(100)]
    [InlineData(int.MaxValue)]
    public void CanEnqueue__КогдаТекущийРазмерНа1МеньшеМаксимального__ДолженРазрешитьДобавление(
        int maxQueueSize)
    {
        var policy = new MaxQueueSizeQueuePolicy(maxQueueSize);
        var queue = Mock.Of<IReadOnlyTaskQueue>(q => q.Count == maxQueueSize - 1);

        var canEnqueue = policy.CanEnqueue(0, Array.Empty<byte>(), queue);

        canEnqueue
           .Should()
           .BeTrue(
                "Максимальный размер {0} еще не достигнут, т.к. сейчас меньше на 1 (только потом уже будет превышен)",
                maxQueueSize);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(int.MaxValue)]
    public void CanEnqueue__КогдаТекущийРазмерРавенМаксимальному__ДолженЗапретитьДобавление(
        int maxQueueSize)
    {
        var policy = new MaxQueueSizeQueuePolicy(maxQueueSize);
        var queue = Mock.Of<IReadOnlyTaskQueue>(q => q.Count == maxQueueSize);

        var canEnqueue = policy.CanEnqueue(0, Array.Empty<byte>(), queue);

        canEnqueue
           .Should()
           .BeFalse("Текущий размер равен максимальному, а значит при добавлении превысит");
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(100, 10)]
    [InlineData(int.MaxValue, int.MaxValue - 1)]
    public void CanEnqueue__КогдаТекущийРазмерБольшеМаксимального__ДолженЗапретитьДобавление(
        int currentSize,
        int maxQueueSize)
    {
        var policy = new MaxQueueSizeQueuePolicy(maxQueueSize);
        var queue = Mock.Of<IReadOnlyTaskQueue>(q => q.Count == currentSize);

        var canEnqueue = policy.CanEnqueue(0, Array.Empty<byte>(), queue);

        canEnqueue
           .Should()
           .BeFalse("Максимальный размер {0} превышен (сейчас размер {1}). Тем более нельзя добавлять", maxQueueSize,
                currentSize);
    }
}