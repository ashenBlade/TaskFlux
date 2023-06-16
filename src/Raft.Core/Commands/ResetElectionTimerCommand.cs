using Raft.Core.StateMachine;

namespace Raft.Core.Commands;

public class ResetElectionTimerCommand: UpdateCommand
{
    public ResetElectionTimerCommand(INodeState previousState, IStateMachine stateMachine) : base(previousState, stateMachine)
    {
    }

    protected override void ExecuteUpdate()
    {
        StateMachine.ElectionTimer.Reset();
    }
}