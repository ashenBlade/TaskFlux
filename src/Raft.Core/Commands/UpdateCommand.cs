using Raft.Core.Node;
using Serilog;

namespace Raft.Core.Commands;

public abstract class UpdateCommand : Command
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
        else
        {
            Serilog.Log.Debug("Состояние уже было обновлено");
        }
    }

    protected abstract void ExecuteUpdate();
}