using Moq;
using TaskFlux.Core.Commands.Enqueue;
using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.PolicyViolation;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Queue;
using TaskFlux.Domain;
using TaskFlux.PriorityQueue;
using Xunit;

namespace TaskFlux.Core.Tests.Commands;

[Trait("Category", "BusinessLogic")]
public class EnqueueCommandTests
{
    private static readonly IEqualityComparer<QueueName> QueueNameComparer = new QueueNameEqualityComparer();

    [Fact]
    public void Apply__КогдаВставкаУспешна__ДолженВернутьEnqueueResponse()
    {
        var command = new EnqueueCommand(0, "hello, world"u8.ToArray(), QueueName.Default);
        var record = new QueueRecord(new RecordId(1), command.Priority, command.Payload);
        var queue = new Mock<ITaskQueue>().Apply(m =>
        {
            m.Setup(q => q.Enqueue(It.IsAny<long>(), It.IsAny<byte[]>()))
                .Returns(record);
        });
        var application = CreateApplication(new EnqueueTaskQueueManager(queue.Object));

        var result = command.Apply(application);

        Assert.IsType<EnqueueResponse>(result);
    }

    [Fact]
    public void Apply__КогдаВставкаУспешна__ДолженВернутьEnqueueResponseСНовойЗаписью()
    {
        var command = new EnqueueCommand(0, "hello, world"u8.ToArray(), QueueName.Default);
        var record = new QueueRecord(new RecordId(1), command.Priority, command.Payload);
        var queue = new Mock<ITaskQueue>().Apply(m =>
        {
            m.Setup(q => q.Enqueue(It.IsAny<long>(), It.IsAny<byte[]>()))
                .Returns(record);
        });
        var application = CreateApplication(new EnqueueTaskQueueManager(queue.Object));

        var result = command.Apply(application);

        var enqueueResponse = Assert.IsType<EnqueueResponse>(result);
        Assert.Equal(record, enqueueResponse.Record);
    }

    [Fact]
    public void Apply__КогдаОчередьНеНайдена__ДолженВернутьErrorResponse()
    {
        var queueName = QueueName.Parse("hello");
        var command = new EnqueueCommand(0, "asdf"u8.ToArray(), queueName);
        var manager = new Mock<ITaskQueueManager>(MockBehavior.Strict).Apply(m =>
        {
            m.Setup(x => x.TryGetQueue(It.Is(queueName, QueueNameComparer), out It.Ref<ITaskQueue>.IsAny))
                .Returns(false);
        });

        var result = command.Apply(new TaskFluxApplication(manager.Object));

        Assert.IsType<ErrorResponse>(result);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    public void Apply__КогдаЕстьНарушеннаяПолитика__ДолженВернутьPolicyViolationResponse(int additionalPolicies)
    {
        var command = new EnqueueCommand(123, "asdf"u8.ToArray(), QueueName.Default);
        var violatedPolicy = new Mock<MaxQueueSizeQueuePolicy>(() => new MaxQueueSizeQueuePolicy(123)).Apply(m =>
        {
            m.Setup(p =>
                    p.CanEnqueue(It.IsAny<long>(), It.IsAny<IReadOnlyList<byte>>(), It.IsAny<IReadOnlyTaskQueue>()))
                .Returns(false);
        });
        var successPolicies = Enumerable.Range(0, additionalPolicies)
            .Select(i => new Mock<MaxQueueSizeQueuePolicy>(i).Apply(m =>
                m.Setup(p => p.CanEnqueue(It.IsAny<long>(),
                        It.IsAny<IReadOnlyList<byte>>(), It.IsAny<IReadOnlyTaskQueue>()))
                    .Returns(true)));
        var policies = successPolicies.Append(violatedPolicy)
            .Select(x => x.Object)
            .Cast<QueuePolicy>()
            .ToArray();
        var queue = new Mock<ITaskQueue>().Apply(m => { m.SetupGet(q => q.Policies).Returns(policies); });
        var manager = new Mock<ITaskQueueManager>().Apply(m =>
        {
            m.Setup(x => x.TryGetQueue(It.IsAny<QueueName>(), out It.Ref<ITaskQueue>.IsAny))
                .Callback((QueueName _, out ITaskQueue foundQueue) => { foundQueue = queue.Object; })
                .Returns(true);
        });

        var result = command.Apply(CreateApplication(manager.Object));

        Assert.IsType<PolicyViolationResponse>(result);
    }

    [Fact]
    public void Apply__КогдаПолитикаНарушена__ДолженВернутьНарушеннуюПолитику()
    {
        var command = new EnqueueCommand(123, "asdf"u8.ToArray(), QueueName.Default);
        var violatedPolicy = new Mock<MaxQueueSizeQueuePolicy>(() => new MaxQueueSizeQueuePolicy(123)).Apply(m =>
        {
            m.Setup(p =>
                    p.CanEnqueue(It.IsAny<long>(), It.IsAny<IReadOnlyList<byte>>(), It.IsAny<IReadOnlyTaskQueue>()))
                .Returns(false);
        });
        var queue = new Mock<ITaskQueue>().Apply(m =>
        {
            m.SetupGet(q => q.Policies).Returns(new QueuePolicy[] { violatedPolicy.Object });
        });
        var manager = new Mock<ITaskQueueManager>().Apply(m =>
        {
            m.Setup(x => x.TryGetQueue(It.IsAny<QueueName>(), out It.Ref<ITaskQueue>.IsAny))
                .Callback((QueueName _, out ITaskQueue foundQueue) => { foundQueue = queue.Object; })
                .Returns(true);
        });

        var result = command.Apply(CreateApplication(manager.Object));

        var policy = ((PolicyViolationResponse)result).ViolatedPolicy;
        Assert.Equal(violatedPolicy.Object, policy, QueuePolicyEqualityComparer.Comparer);
    }


    private class EnqueueTaskQueueManager : ITaskQueueManager
    {
        private readonly ITaskQueue _queue;

        public EnqueueTaskQueueManager(ITaskQueue queue)
        {
            _queue = queue;
        }

        public bool HasQueue(QueueName name)
        {
            return _queue.Name == name;
        }

        public IReadOnlyCollection<ITaskQueueMetadata> GetAllQueuesMetadata()
        {
            throw new InvalidOperationException("Метод не должен быть вызван");
        }

        public int QueuesCount => 1;

        public bool TryGetQueue(QueueName name, out ITaskQueue taskQueue)
        {
            taskQueue = _queue;
            return _queue.Name == name;
        }

        public bool TryAddQueue(QueueName name, ITaskQueue taskQueue)
        {
            throw new InvalidOperationException("Метод не должен быть вызван");
        }

        public ITaskQueueBuilder CreateBuilder(QueueName name, PriorityQueueCode code)
        {
            throw new InvalidOperationException("Метод не должен быть вызван");
        }

        public bool TryDeleteQueue(QueueName name, out ITaskQueue deleted)
        {
            throw new InvalidOperationException("Метод не должен быть вызван");
        }

        public IReadOnlyCollection<ITaskQueue> GetAllQueues()
        {
            throw new InvalidOperationException("Метод не должен быть вызван");
        }

        IReadOnlyCollection<IReadOnlyTaskQueue> IReadOnlyTaskQueueManager.GetAllQueues()
        {
            return GetAllQueues();
        }

        public bool TryGetQueue(QueueName name, out IReadOnlyTaskQueue taskQueue)
        {
            taskQueue = _queue;
            return _queue.Name == name;
        }
    }

    private static IApplication CreateApplication(ITaskQueueManager manager) =>
        new Mock<IApplication>()
            .Apply(m => m.SetupGet(x => x.TaskQueueManager).Returns(manager))
            .Object;
}