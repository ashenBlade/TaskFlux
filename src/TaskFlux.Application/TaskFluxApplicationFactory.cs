using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Core.Commands;
using TaskFlux.Core.Queue;
using TaskFlux.Core.Restore;
using TaskFlux.Core.Subscription;
using TaskFlux.Persistence.ApplicationState;
using TaskFlux.Persistence.ApplicationState.Deltas;

namespace TaskFlux.Application;

public class TaskFluxApplicationFactory : IApplicationFactory<Command, Response>
{
    private readonly IQueueSubscriberManagerFactory _queueSubscriberManagerFactory;

    public TaskFluxApplicationFactory(IQueueSubscriberManagerFactory queueSubscriberManagerFactory)
    {
        ArgumentNullException.ThrowIfNull(queueSubscriberManagerFactory);

        _queueSubscriberManagerFactory = queueSubscriberManagerFactory;
    }

    public IApplication<Command, Response> Restore(ISnapshot? snapshot, IEnumerable<byte[]> deltas)
    {
        var collection = GetQueueCollection(snapshot);

        foreach (var deltaBytes in deltas)
        {
            var delta = Delta.DeserializeFrom(deltaBytes);
            delta.Apply(collection);
        }

        var manager = TaskQueueManager.CreateFrom(collection, _queueSubscriberManagerFactory);
        var application = new TaskFluxApplication(manager);
        return new ProxyTaskFluxApplication(application);
    }

    private static QueueCollection GetQueueCollection(ISnapshot? snapshot)
    {
        if (snapshot is not null)
        {
            using var stream = new SnapshotStream(snapshot);
            return QueuesSnapshotSerializer.Deserialize(stream);
        }

        // Создаем новое начальное состояние с единственной очередью по умолчанию
        return QueueCollection.CreateDefault();
    }

    public ISnapshot CreateSnapshot(ISnapshot? previousState, IEnumerable<byte[]> deltas)
    {
        var collection = StateRestorer.RestoreState(previousState, deltas);
        return new QueueCollectionSnapshot(collection);
    }
}