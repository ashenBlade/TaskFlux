using Raft.StateMachine.JobQueue.Commands;

namespace Raft.StateMachine.JobQueue.Serialization;

public interface IJobQueueRequestDeserializer
{
    IJobQueueRequest Deserialize(byte[] payload);
}