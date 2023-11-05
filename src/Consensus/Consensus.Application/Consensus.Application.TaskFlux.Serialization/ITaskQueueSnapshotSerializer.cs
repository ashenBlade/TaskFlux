using TaskFlux.Core.Queue;

namespace Consensus.Application.TaskFlux.Serialization;

public interface ITaskQueueSnapshotSerializer
{
    public byte[] Serialize(ITaskQueue queue);
    public void Serialize(Stream destination, IEnumerable<ITaskQueue> queues, CancellationToken token = default);
    public IEnumerable<ITaskQueue> Deserialize(Stream source, CancellationToken token = default);
}