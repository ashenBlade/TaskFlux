using Raft.Core.StateMachine;

namespace Raft.Core.Commands;

public class StartHeartbeatTimerCommand: UpdateCommand
{
    public StartHeartbeatTimerCommand(INodeState previousState, IStateMachine stateMachine) : base(previousState, stateMachine)
    {
    }

    protected override void ExecuteUpdate()
    {
        StateMachine.HeartbeatTimer.Start();
    }
}