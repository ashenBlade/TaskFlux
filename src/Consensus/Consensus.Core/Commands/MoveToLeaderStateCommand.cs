using Consensus.Core.State;
using Consensus.Core.State.LeaderState;

namespace Consensus.Core.Commands;

internal class MoveToLeaderStateCommand: UpdateCommand
{
    public MoveToLeaderStateCommand(IConsensusModuleState previousState, IConsensusModule consensusModule)
        : base(previousState, consensusModule)
    { }

    protected override void ExecuteUpdate()
    {
        ConsensusModule.CurrentState = LeaderState.Create(ConsensusModule);
        ConsensusModule.HeartbeatTimer.Start();
        ConsensusModule.ElectionTimer.Stop();
    }
}