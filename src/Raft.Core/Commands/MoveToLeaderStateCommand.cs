using Raft.Core.StateMachine;

namespace Raft.Core.Commands;

public class MoveToLeaderStateCommand: UpdateCommand
{
    public MoveToLeaderStateCommand(INodeState previousState, IStateMachine stateMachine) : base(previousState, stateMachine)
    { }

    protected override void ExecuteUpdate()
    {
        StateMachine.CurrentState = LeaderState.Create(StateMachine);
        StateMachine.HeartbeatTimer.Start();
        StateMachine.ElectionTimer.Stop();
    }
}