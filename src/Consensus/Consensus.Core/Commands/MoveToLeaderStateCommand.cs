using Consensus.Core.State;
using Consensus.Core.State.LeaderState;

namespace Consensus.Core.Commands;

internal class MoveToLeaderStateCommand<TCommand, TResponse>: UpdateCommand<TCommand, TResponse>
{
    public MoveToLeaderStateCommand(IConsensusModuleState<TCommand, TResponse> previousState, IConsensusModule<TCommand, TResponse> consensusModule)
        : base(previousState, consensusModule)
    { }

    protected override void ExecuteUpdate()
    {
        ConsensusModule.CurrentState = LeaderState.Create(ConsensusModule);
        ConsensusModule.HeartbeatTimer.Start();
        ConsensusModule.ElectionTimer.Stop();
    }
}