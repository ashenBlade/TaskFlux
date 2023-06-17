using Raft.Core.Node;

namespace Raft.Core.Commands;

public class MoveToLeaderStateCommand: UpdateCommand
{
    public MoveToLeaderStateCommand(INodeState previousState, INode node)
        : base(previousState, node)
    { }

    protected override void ExecuteUpdate()
    {
        Node.CurrentState = LeaderState.Create(Node);
        Node.HeartbeatTimer.Start();
        Node.ElectionTimer.Stop();
    }
}