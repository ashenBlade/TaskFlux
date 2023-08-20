namespace Consensus.Core.Commands.InstallSnapshot;

public class InstallSnapshotCommand<TCommand, TResponse> : Command<InstallSnapshotResponse, TCommand, TResponse>
{
    public InstallSnapshotRequest Request { get; }

    public InstallSnapshotCommand(InstallSnapshotRequest request, IConsensusModule<TCommand, TResponse> consensusModule)
        : base(consensusModule)
    {
        Request = request;
    }

    public override InstallSnapshotResponse Execute()
    {
        return ConsensusModule.CurrentState.Apply(Request);
    }
}