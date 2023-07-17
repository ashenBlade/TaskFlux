using Raft.Core.State;

namespace Raft.Core.Commands;

internal class MoveToCandidateAfterElectionTimerTimeoutCommand: UpdateCommand
{
    public MoveToCandidateAfterElectionTimerTimeoutCommand(IConsensusModuleState previousState, IConsensusModule consensusModule) 
        : base(previousState, consensusModule)
    { }

    protected override void ExecuteUpdate()
    {
        ConsensusModule.ElectionTimer.Stop();
        ConsensusModule.CurrentState = CandidateState.Create(ConsensusModule);
        ConsensusModule.UpdateState(ConsensusModule.CurrentTerm.Increment(), ConsensusModule.Id);
        ConsensusModule.ElectionTimer.Start();
    }
}