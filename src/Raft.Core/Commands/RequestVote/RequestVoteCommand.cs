namespace Raft.Core.Commands.RequestVote;

internal class RequestVoteCommand: Command<RequestVoteResponse>
{
    private readonly RequestVoteRequest _request;

    public RequestVoteCommand(RequestVoteRequest request, IConsensusModule consensusModule) : base(consensusModule)
    {
        _request = request;
    }

    public override RequestVoteResponse Execute()
    {
        return ConsensusModule.CurrentState.Apply(_request);
    }
}