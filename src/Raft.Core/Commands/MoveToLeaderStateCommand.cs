using Raft.Core.Node;
using Raft.Core.Node.LeaderState;

namespace Raft.Core.Commands;

internal class MoveToLeaderStateCommand: UpdateCommand
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