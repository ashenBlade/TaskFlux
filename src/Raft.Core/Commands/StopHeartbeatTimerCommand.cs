using Raft.Core.Node;

namespace Raft.Core.Commands;

internal class StopHeartbeatTimerCommand: UpdateCommand
{
    public StopHeartbeatTimerCommand(INodeState previousState, INode node) 
        : base(previousState, node)
    { }

    protected override void ExecuteUpdate()
    {
        Node.HeartbeatTimer.Stop();
    }
}