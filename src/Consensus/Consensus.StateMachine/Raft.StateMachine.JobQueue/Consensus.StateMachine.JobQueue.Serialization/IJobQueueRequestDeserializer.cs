using Consensus.StateMachine.JobQueue.Commands;

namespace Consensus.StateMachine.JobQueue.Serialization;

public interface IJobQueueRequestDeserializer
{
    IJobQueueRequest Deserialize(byte[] payload);
}