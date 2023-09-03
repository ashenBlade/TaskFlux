using JobQueue.Core;

namespace Consensus.StateMachine.TaskFlux.Serialization;

public interface IJobQueueSnapshotSerializer
{
    public byte[] Serialize(IJobQueue queue);
    public void Serialize(Stream destination, IEnumerable<IJobQueue> queues, CancellationToken token = default);
    public IEnumerable<IJobQueue> Deserialize(Stream source, CancellationToken token = default);
}