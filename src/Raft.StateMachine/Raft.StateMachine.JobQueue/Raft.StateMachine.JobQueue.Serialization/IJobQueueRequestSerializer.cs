using Raft.StateMachine.JobQueue.Commands;

namespace Raft.StateMachine.JobQueue.Serialization;

public interface IJobQueueRequestSerializer
{
    void Serialize(IJobQueueRequest request, BinaryWriter writer);
}