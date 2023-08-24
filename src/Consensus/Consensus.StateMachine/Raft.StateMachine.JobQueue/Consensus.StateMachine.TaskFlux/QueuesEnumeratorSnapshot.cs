using Consensus.Core.Log;
using Consensus.Core.Persistence;
using Consensus.StateMachine.TaskFlux.Serialization;
using JobQueue.Core;

namespace Consensus.StateMachine.TaskFlux;

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

    public void WriteTo(Stream stream, CancellationToken token = default)
    {
        var queues = _manager.GetAllQueues();
        _serializer.Serialize(stream, queues, token);
    }
}