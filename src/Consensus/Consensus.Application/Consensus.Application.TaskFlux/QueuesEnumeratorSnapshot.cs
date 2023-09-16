using Consensus.Application.TaskFlux.Serialization;
using Consensus.Raft.Persistence;
using JobQueue.Core;

namespace Consensus.Application.TaskFlux;

/// <summary>
/// Реализация снапшота, которая итерируется по всем очередям и поочередно сериализует каждую 
/// </summary>
public class QueuesEnumeratorSnapshot : ISnapshot
{
    private readonly IJobQueueManager _manager;
    private readonly IJobQueueSnapshotSerializer _serializer;

    public QueuesEnumeratorSnapshot(IJobQueueManager manager, IJobQueueSnapshotSerializer serializer)
    {
        _manager = manager;
        _serializer = serializer;
    }


    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        var queues = _manager.GetAllQueues();
        foreach (var queue in queues)
        {
            var buffer = _serializer.Serialize(queue);
            yield return buffer;
        }
    }
}