using Raft.Core.Node;

namespace Raft.Core.Commands;

public class StopHeartbeatTimerCommand: UpdateCommand
{
    public StopHeartbeatTimerCommand(INodeState previousState, INode node) : base(previousState, node)
    {
    }

    protected override void ExecuteUpdate()
    {
        Node.HeartbeatTimer.Stop();
    }
}