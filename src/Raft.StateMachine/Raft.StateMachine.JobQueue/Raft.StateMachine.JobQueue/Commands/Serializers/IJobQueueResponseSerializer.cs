namespace Raft.StateMachine.JobQueue.Commands.Serializers;

public interface IJobQueueResponseSerializer
{
    void Serialize(IJobQueueResponse response, BinaryWriter writer);
}