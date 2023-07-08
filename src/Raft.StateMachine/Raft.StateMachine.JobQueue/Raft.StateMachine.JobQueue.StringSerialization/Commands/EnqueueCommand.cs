using Raft.StateMachine.JobQueue.Requests;
using Raft.StateMachine.JobQueue.StringSerialization.Responses;

namespace Raft.StateMachine.JobQueue.StringSerialization.Commands;

public class EnqueueCommand: ICommand
{
    private readonly EnqueueRequest _request;

    public EnqueueCommand(EnqueueRequest request)
    {
        _request = request;
    }
    public IResponse Apply(IJobQueueStateMachine stateMachine)
    {
        return new EnqueueStringResponse(stateMachine.Apply(_request));
    }

    public void ApplyNoResponse(IJobQueueStateMachine stateMachine)
    { }
}