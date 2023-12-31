using Consensus.Core;
using Consensus.Raft;
using TaskFlux.Commands;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Models;
using TaskFlux.PriorityQueue;
using TaskFlux.Serialization;

namespace Consensus.Application.TaskFlux;

public class TaskFluxApplicationFactory : IApplicationFactory<Command, Response>
{
    private readonly INodeInfo _nodeInfo;
    private readonly IApplicationInfo _applicationInfo;
    private readonly IClusterInfo _clusterInfo;

    public TaskFluxApplicationFactory(
        INodeInfo nodeInfo,
        IApplicationInfo applicationInfo,
        IClusterInfo clusterInfo)
    {
        _nodeInfo = nodeInfo;
        _applicationInfo = applicationInfo;
        _clusterInfo = clusterInfo;
    }

    public IApplication<Command, Response> Restore(ISnapshot? snapshot, IEnumerable<byte[]> deltas)
    {
        var collection = GetQueueCollection(snapshot);

        foreach (var deltaBytes in deltas)
        {
            var delta = Delta.DeserializeFrom(deltaBytes);
            delta.Apply(collection);
        }

        var manager = CreateManager(collection);
        var application = new TaskFluxApplication(_nodeInfo, _clusterInfo, _applicationInfo, manager);
        return new ProxyTaskFluxApplication(application);
    }

    private static QueueCollection GetQueueCollection(ISnapshot? snapshot)
    {
        QueueCollection collection;
        if (snapshot is not null)
        {
            // Восстанавливаем состояние из снапшота
            var stream = new MemoryStream();
            foreach (var memory in snapshot.GetAllChunks())
            {
                stream.Write(memory.Span);
            }

            stream.Position = 0;

            collection = QueuesSnapshotSerializer.Deserialize(stream);
        }
        else
        {
            // Совершенно пустое начальное состояние
            collection = new QueueCollection();

            // Всегда должна быть очередь по умолчанию
            collection.CreateQueue(QueueName.Default, PriorityQueueCode.Heap4Arity, null, null, null);
        }

        return collection;
    }

    private static TaskQueueManager CreateManager(QueueCollection collection)
    {
        var queues = new List<ITaskQueue>(collection.Count);
        foreach (var (name, code, maxQueueSize, maxPayloadSize, priorityRange, data) in collection.GetQueuesRaw())
        {
            var builder = new TaskQueueBuilder(name, code)
                         .WithMaxQueueSize(maxQueueSize)
                         .WithMaxPayloadSize(maxPayloadSize)
                         .WithPriorityRange(priorityRange)
                         .WithData(data);
            queues.Add(builder.Build());
        }

        return new TaskQueueManager(queues);
    }

    public ISnapshot CreateSnapshot(ISnapshot? previousState, IEnumerable<byte[]> deltas)
    {
        var collection = StateRestorer.RestoreState(previousState, deltas);
        return new QueueCollectionSnapshot(collection);
    }
}