using System.Windows.Input;
using Raft.Core.StateMachine;

namespace Raft.Core.Commands;

public class MoveToCandidateAfterElectionTimerTimeoutCommand: UpdateCommand
{
    public MoveToCandidateAfterElectionTimerTimeoutCommand(INodeState previousState, IStateMachine stateMachine) 
        : base(previousState, stateMachine)
    { }

    protected override void ExecuteUpdate()
    {
        StateMachine.ElectionTimer.Stop();
        StateMachine.CurrentState = CandidateState.Create(StateMachine);
        StateMachine.CurrentTerm = StateMachine.CurrentTerm.Increment();
        StateMachine.VotedFor = StateMachine.Id;
        StateMachine.ElectionTimer.Start();
    }
}