namespace Raft.Core.Commands.AppendEntries;

public class AppendEntriesCommand : Command<AppendEntriesResponse>
{
    private readonly AppendEntriesRequest _request;

    public AppendEntriesCommand(AppendEntriesRequest request, IConsensusModule consensusModule) : base(consensusModule)
    {
        _request = request;
    }

    public override AppendEntriesResponse Execute()
    {
        return ConsensusModule.CurrentState.Apply(_request);
    }
}
