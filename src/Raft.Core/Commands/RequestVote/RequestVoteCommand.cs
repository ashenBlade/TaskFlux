using Raft.Core.StateMachine;

namespace Raft.Core.Commands.RequestVote;

public class RequestVoteCommand: Command<RequestVoteResponse>
{
    private readonly RequestVoteRequest _request;

    public RequestVoteCommand(RequestVoteRequest request, IStateMachine stateMachine): base(stateMachine)
    {
        _request = request;
    }
    
    public override RequestVoteResponse Execute()
    {
        return StateMachine.CurrentState.Apply(_request);
    }
}