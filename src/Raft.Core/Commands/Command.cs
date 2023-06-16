using System.Windows.Input;
using Raft.Core.StateMachine;

namespace Raft.Core.Commands;

public abstract class Command<T>: ICommand<T>
{
    protected IStateMachine StateMachine { get; }
    protected Command(IStateMachine stateMachine)
    {
        StateMachine = stateMachine;
    }
    public abstract T Execute();
}

public abstract class Command : ICommand
{
    protected IStateMachine StateMachine { get; }
    protected Command(IStateMachine stateMachine)
    {
        StateMachine = stateMachine;
    }
    public abstract void Execute();
}
