using Raft.Core.State;
using Raft.Core.State.LeaderState;

namespace Raft.Core.Commands;

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