namespace Consensus.Core.Commands.InstallSnapshot;

public class InstallSnapshotCommand<TCommand, TResponse> : Command<InstallSnapshotResponse, TCommand, TResponse>
{
    private readonly CancellationToken _token;
    public InstallSnapshotRequest Request { get; }

    public InstallSnapshotCommand(InstallSnapshotRequest request, 
                                  IConsensusModule<TCommand, TResponse> consensusModule,
                                  CancellationToken token)
        : base(consensusModule)
    {
        _token = token;
        Request = request;
    }

    public override InstallSnapshotResponse Execute()
    {
        return ConsensusModule.CurrentState.Apply(Request, _token);
    }
}