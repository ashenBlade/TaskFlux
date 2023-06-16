using Raft.Core.StateMachine;
using Serilog;

namespace Raft.Core.Commands;

public abstract class UpdateCommand : Command
{
    private readonly INodeState _previousState;

    protected UpdateCommand(INodeState previousState, IStateMachine stateMachine)
    :base(stateMachine)
    {
        _previousState = previousState;
    }
    
    public override void Execute()
    {
        if (StateMachine.CurrentState == _previousState)
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