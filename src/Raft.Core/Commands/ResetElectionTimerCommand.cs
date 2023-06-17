using Raft.Core.Node;

namespace Raft.Core.Commands;

public class ResetElectionTimerCommand: UpdateCommand
{
    public ResetElectionTimerCommand(INodeState previousState, INode node) : base(previousState, node)
    {
    }

    protected override void ExecuteUpdate()
    {
        Node.ElectionTimer.Reset();
    }
}