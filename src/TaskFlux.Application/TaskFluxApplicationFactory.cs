using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Core.Commands;
using TaskFlux.Core.Queue;
using TaskFlux.Core.Subscription;
using TaskFlux.Persistence.ApplicationState;

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
        var collection = StateRestorer.RestoreState(snapshot, deltas);
        var manager = TaskQueueManager.CreateFrom(collection, _queueSubscriberManagerFactory);
        var application = new TaskFluxApplication(manager);
        return new ProxyTaskFluxApplication(application);
    }

    public ISnapshot CreateSnapshot(ISnapshot? previousState, IEnumerable<byte[]> deltas)
    {
        var collection = StateRestorer.RestoreState(previousState, deltas);
        return new QueueCollectionSnapshot(collection);
    }
}