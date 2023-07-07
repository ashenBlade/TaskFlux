namespace Raft.StateMachine.JobQueue.Requests;

public interface IJobQueueResponse
{
    public void Apply(BinaryWriter writer);
}