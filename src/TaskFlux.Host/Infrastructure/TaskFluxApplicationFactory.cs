using Consensus.Application.TaskFlux;
using Consensus.Application.TaskFlux.Serialization;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using TaskFlux.Commands;
using TaskFlux.Core;
using TaskFlux.Host.Helpers;
using TaskFlux.Node;
using TaskQueue.Core;
using TaskQueue.Models;
using TaskQueue.Serialization;
using Result = TaskFlux.Commands.Result;

namespace TaskFlux.Host.Infrastructure;

public class TaskFluxApplicationFactory : IApplicationFactory<Command, Result>
{
    private readonly INodeInfo _nodeInfo;
    private readonly IApplicationInfo _appInfo;
    private readonly IClusterInfo _clusterInfo;

    private readonly ITaskQueueSnapshotSerializer _fileTaskQueueSnapshotSerializer =
        new FileTaskQueueSnapshotSerializer(BuilderTaskQueueFactory.Instance);

    public TaskFluxApplicationFactory(INodeInfo nodeInfo, IApplicationInfo appInfo, IClusterInfo clusterInfo)
    {
        _nodeInfo = nodeInfo;
        _appInfo = appInfo;
        _clusterInfo = clusterInfo;
    }

    public IApplication<Command, Result> CreateEmpty()
    {
        var queue = new TaskQueueBuilder(QueueName.Default)
           .Build();
        var node = new TaskFluxNode(new TaskQueueManager(queue));
        var commandContext = new CommandContext(node, _nodeInfo, _appInfo, _clusterInfo);
        return new TaskFluxApplication(commandContext, _fileTaskQueueSnapshotSerializer);
    }

    public IApplication<Command, Result> Restore(ISnapshot snapshot)
    {
        var memoryStream = new MemoryStream();
        foreach (var chunk in snapshot.GetAllChunks())
        {
            memoryStream.Write(chunk.Span);
        }

        memoryStream.Position = 0;
        var queues = _fileTaskQueueSnapshotSerializer.Deserialize(memoryStream)
                                                     .ToList();

        var node = new TaskFluxNode(new TaskQueueManager(queues));
        return new TaskFluxApplication(new CommandContext(node, _nodeInfo, _appInfo, _clusterInfo),
            _fileTaskQueueSnapshotSerializer);
    }
}