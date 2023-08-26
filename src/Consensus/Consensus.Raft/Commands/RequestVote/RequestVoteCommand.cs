namespace Consensus.Raft.Commands.RequestVote;

internal class RequestVoteCommand<TCommand, TResponse>: Command<RequestVoteResponse, TCommand, TResponse>
{
    private readonly RequestVoteRequest _request;

    public RequestVoteCommand(RequestVoteRequest request, IConsensusModule<TCommand, TResponse> consensusModule) : base(consensusModule)
    {
        _request = request;
    }

    public override RequestVoteResponse Execute()
    {
        return ConsensusModule.CurrentState.Apply(_request);
    }
}