using Raft.Core.Node;
using Serilog;

namespace Raft.Core.Commands;

internal abstract class UpdateCommand : Command
{
    private readonly INodeState _previousState;

    protected UpdateCommand(INodeState previousState, INode node)
    :base(node)
    {
        _previousState = previousState;
    }
    
    public override void Execute()
    {
        if (Node.CurrentState == _previousState)
        {
            ExecuteUpdate();
        }
    }

    protected abstract void ExecuteUpdate();
}