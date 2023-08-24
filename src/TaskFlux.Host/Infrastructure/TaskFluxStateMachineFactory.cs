using Consensus.Core;
using Consensus.Core.Persistence;
using Consensus.StateMachine.TaskFlux;
using Consensus.StateMachine.TaskFlux.Serialization;
using JobQueue.Core;
using JobQueue.InMemory;
using JobQueue.PriorityQueue.StandardLibrary;
using JobQueue.Serialization;
using TaskFlux.Commands;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Host.Helpers;
using TaskFlux.Node;

namespace TaskFlux.Host.Infrastructure;

public class TaskFluxStateMachineFactory: IStateMachineFactory<Command, Result>
{
    private readonly INodeInfo _nodeInfo;
    private readonly IApplicationInfo _appInfo;
    private readonly IClusterInfo _clusterInfo;
    private readonly IJobQueueSnapshotSerializer _fileJobQueueSnapshotSerializer = new FileJobQueueSnapshotSerializer(PrioritizedJobQueueFactory.Instance);

    public TaskFluxStateMachineFactory(INodeInfo nodeInfo, IApplicationInfo appInfo, IClusterInfo clusterInfo)
    {
        _nodeInfo = nodeInfo;
        _appInfo = appInfo;
        _clusterInfo = clusterInfo;
    }
    
    public IStateMachine<Command, Result> CreateEmpty()
    {
        var node = new TaskFluxNode(new SimpleJobQueueManager(new PrioritizedJobQueue(QueueName.Default, 0, new StandardLibraryPriorityQueue<long, byte[]>())));
        var commandContext = new CommandContext(node, _nodeInfo, _appInfo, _clusterInfo);
        var serializer = _fileJobQueueSnapshotSerializer;
        return new TaskFluxStateMachine(commandContext, serializer);
    }

    public IStateMachine<Command, Result> Restore(ISnapshot snapshot)
    {
        var memoryStream = new MemoryStream();
        snapshot.WriteTo(memoryStream);
        var queues = _fileJobQueueSnapshotSerializer.Deserialize(memoryStream)
                                                    .ToList();

        var node = new TaskFluxNode(new SimpleJobQueueManager(queues));
        return new TaskFluxStateMachine(new CommandContext(node, _nodeInfo, _appInfo, _clusterInfo), 
            _fileJobQueueSnapshotSerializer);
    }
}