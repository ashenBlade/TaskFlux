using Raft.Core.StateMachine;

namespace Raft.Core.Commands;

public class StopHeartbeatTimerCommand: UpdateCommand
{
    public StopHeartbeatTimerCommand(INodeState previousState, IStateMachine stateMachine) : base(previousState, stateMachine)
    {
    }

    protected override void ExecuteUpdate()
    {
        StateMachine.HeartbeatTimer.Stop();
    }
}