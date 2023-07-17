using Consensus.StateMachine.JobQueue.Commands;

namespace Consensus.StateMachine.JobQueue.Serialization;

public interface IJobQueueRequestSerializer
{
    void Serialize(IJobQueueRequest request, BinaryWriter writer);
}