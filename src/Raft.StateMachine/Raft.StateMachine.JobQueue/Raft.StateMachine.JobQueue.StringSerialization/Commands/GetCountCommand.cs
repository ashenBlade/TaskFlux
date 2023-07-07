using Raft.StateMachine.JobQueue.Requests;
using Raft.StateMachine.JobQueue.StringSerialization.Responses;

namespace Raft.StateMachine.JobQueue.StringSerialization.Commands;

public class GetCountCommand: ICommand
{
    private readonly GetCountRequest _request;

    public GetCountCommand(GetCountRequest request)
    {
        _request = request;
    }
    
    public IResponse Apply(IJobQueueStateMachine stateMachine)
    {
        return new GetCountStringResponse(stateMachine.Apply(_request));
    }
}