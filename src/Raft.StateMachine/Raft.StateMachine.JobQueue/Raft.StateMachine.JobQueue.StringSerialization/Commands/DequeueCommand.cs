using Raft.StateMachine.JobQueue.Requests;
using Raft.StateMachine.JobQueue.StringSerialization.Responses;

namespace Raft.StateMachine.JobQueue.StringSerialization.Commands;

public class DequeueCommand: ICommand
{
    private readonly DequeueRequest _request;

    public DequeueCommand(DequeueRequest request)
    {
        _request = request;
    }
    public IResponse Apply(IJobQueueStateMachine stateMachine)
    {
        var response = stateMachine.Apply(_request);
        return new DequeueStringResponse(response);
    }

    public void ApplyNoResponse(IJobQueueStateMachine stateMachine)
    {
        stateMachine.Apply(_request);
    }
}