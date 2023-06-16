using Raft.Core.StateMachine;

namespace Raft.Core.Commands.Heartbeat;

public class HeartbeatCommand: Command<HeartbeatResponse>
{
    private readonly HeartbeatRequest _request;

    public HeartbeatCommand(HeartbeatRequest request, IStateMachine stateMachine)
        :base(stateMachine)
    {
        _request = request;
    }
    
    public override HeartbeatResponse Execute()
    {
        return StateMachine.CurrentState.Apply(_request);
    }
}