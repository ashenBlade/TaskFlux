namespace Consensus.Raft.Commands.AppendEntries;

public class AppendEntriesCommand<TCommand, TResponse> : Command<AppendEntriesResponse, TCommand, TResponse>
{
    private readonly AppendEntriesRequest _request;

    public AppendEntriesCommand(AppendEntriesRequest request, IConsensusModule<TCommand, TResponse> consensusModule) :
        base(consensusModule)
    {
        _request = request;
    }

    public override AppendEntriesResponse Execute()
    {
        return ConsensusModule.CurrentState.Apply(_request);
    }
}