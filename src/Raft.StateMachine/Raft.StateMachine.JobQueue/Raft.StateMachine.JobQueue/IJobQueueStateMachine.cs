using Raft.StateMachine.JobQueue.Requests;

namespace Raft.StateMachine.JobQueue;

public interface IJobQueueStateMachine
{
    public DequeueResponse Apply(DequeueRequest request);
    public EnqueueResponse Apply(EnqueueRequest request);
    public GetCountResponse Apply(GetCountRequest request);
}