using JobQueue.Core;

namespace Consensus.StateMachine.TaskFlux.Serialization;

public interface IJobQueueSnapshotSerializer
{
    public void Serialize(Stream destination, IJobQueue queue, CancellationToken token = default);
}