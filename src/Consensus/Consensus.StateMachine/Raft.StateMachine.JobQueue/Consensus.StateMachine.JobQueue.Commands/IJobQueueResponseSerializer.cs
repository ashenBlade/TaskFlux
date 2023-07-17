namespace Consensus.StateMachine.JobQueue.Commands;

public interface IJobQueueResponseSerializer
{
    void Serialize(IJobQueueResponse response, BinaryWriter writer);
}