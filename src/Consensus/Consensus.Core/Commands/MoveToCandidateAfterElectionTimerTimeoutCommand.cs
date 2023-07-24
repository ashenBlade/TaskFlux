using Consensus.Core.State;

namespace Consensus.Core.Commands;

internal class MoveToCandidateAfterElectionTimerTimeoutCommand<TCommand, TResponse>: UpdateCommand<TCommand, TResponse>
{
    public MoveToCandidateAfterElectionTimerTimeoutCommand(
        ConsensusModuleState<TCommand, TResponse> previousState, 
        IConsensusModule<TCommand, TResponse> consensusModule) 
        : base(previousState, consensusModule)
    { }

    protected override void ExecuteUpdate()
    {
        ConsensusModule.ElectionTimer.Stop();
        ConsensusModule.CurrentState = ConsensusModule.CreateCandidateState();
        ConsensusModule.UpdateState(ConsensusModule.CurrentTerm.Increment(), ConsensusModule.Id);
        ConsensusModule.ElectionTimer.Start();
    }
}