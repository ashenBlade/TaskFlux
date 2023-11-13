using Consensus.Application.TaskFlux;
using Consensus.Application.TaskFlux.Serialization;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using TaskFlux.Commands;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.Host.Helpers;
using TaskFlux.Models;
using TaskFlux.Serialization;

namespace TaskFlux.Host.Infrastructure;

public class TaskFluxApplicationFactory : IApplicationFactory<Command, Response>
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

    public IApplication<Command, Response> CreateEmpty()
    {
        var queue = new TaskQueueBuilder(QueueName.Default)
           .Build();
        var queueManager = new TaskQueueManager(queue);
        var application = new ProxyTaskFluxApplication(
            new TaskFluxApplication(_nodeInfo, _clusterInfo, _appInfo, queueManager), _fileTaskQueueSnapshotSerializer);
        return application;
    }

    public IApplication<Command, Response> Restore(ISnapshot snapshot)
    {
        // Читаем снапшот (потом надо сделать свой поток-обертку)
        var memoryStream = new MemoryStream();
        foreach (var chunk in snapshot.GetAllChunks())
        {
            memoryStream.Write(chunk.Span);
        }

        // Откатываемся в начало снапшота
        memoryStream.Position = 0;

        // Десериализуем
        var queues = _fileTaskQueueSnapshotSerializer
                    .Deserialize(memoryStream)
                    .ToList();

        // Создаем основные объекты
        var node = new TaskQueueManager(queues);
        return new ProxyTaskFluxApplication(new TaskFluxApplication(_nodeInfo, _clusterInfo, _appInfo, node),
            _fileTaskQueueSnapshotSerializer);
    }
}