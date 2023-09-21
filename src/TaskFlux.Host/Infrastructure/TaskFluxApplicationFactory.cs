using Consensus.Application.TaskFlux;
using Consensus.Application.TaskFlux.Serialization;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using JobQueue.Core;
using JobQueue.InMemory;
using JobQueue.PriorityQueue.StandardLibrary;
using JobQueue.Serialization;
using TaskFlux.Commands;
using TaskFlux.Core;
using TaskFlux.Host.Helpers;
using TaskFlux.Node;

namespace TaskFlux.Host.Infrastructure;

public class TaskFluxApplicationFactory : IApplicationFactory<Command, Result>
{
    private readonly INodeInfo _nodeInfo;
    private readonly IApplicationInfo _appInfo;
    private readonly IClusterInfo _clusterInfo;

    private readonly IJobQueueSnapshotSerializer _fileJobQueueSnapshotSerializer =
        new FileJobQueueSnapshotSerializer(PrioritizedJobQueueFactory.Instance);

    public TaskFluxApplicationFactory(INodeInfo nodeInfo, IApplicationInfo appInfo, IClusterInfo clusterInfo)
    {
        _nodeInfo = nodeInfo;
        _appInfo = appInfo;
        _clusterInfo = clusterInfo;
    }

    public IApplication<Command, Result> CreateEmpty()
    {
        var node = new TaskFluxNode(new SimpleJobQueueManager(new PrioritizedJobQueue(QueueName.Default, 0,
            new StandardLibraryPriorityQueue<long, byte[]>())));
        var commandContext = new CommandContext(node, _nodeInfo, _appInfo, _clusterInfo);
        var serializer = _fileJobQueueSnapshotSerializer;
        return new TaskFluxApplication(commandContext, serializer);
    }

    public IApplication<Command, Result> Restore(ISnapshot snapshot)
    {
        var memoryStream = new MemoryStream();
        foreach (var chunk in snapshot.GetAllChunks())
        {
            memoryStream.Write(chunk.Span);
        }

        memoryStream.Position = 0;
        var queues = _fileJobQueueSnapshotSerializer.Deserialize(memoryStream)
                                                    .ToList();

        var node = new TaskFluxNode(new SimpleJobQueueManager(queues));
        return new TaskFluxApplication(new CommandContext(node, _nodeInfo, _appInfo, _clusterInfo),
            _fileJobQueueSnapshotSerializer);
    }
}