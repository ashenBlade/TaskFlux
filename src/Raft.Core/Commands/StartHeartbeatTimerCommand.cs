using Raft.Core.Node;

namespace Raft.Core.Commands;

internal class StartHeartbeatTimerCommand: UpdateCommand
{
    public StartHeartbeatTimerCommand(INodeState previousState, INode node) 
        : base(previousState, node)
    { }

    protected override void ExecuteUpdate()
    {
        Node.HeartbeatTimer.Start();
    }
}