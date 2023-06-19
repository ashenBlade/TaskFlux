using System.Windows.Input;
using Raft.Core.Node;

namespace Raft.Core.Commands;

public abstract class Command<T>: ICommand<T>
{
    protected INode Node { get; }
    protected Command(INode node)
    {
        Node = node;
    }
    public abstract T Execute();
}

public abstract class Command : ICommand
{
    protected INode Node { get; }
    protected Command(INode node)
    {
        Node = node;
    }
    public abstract void Execute();
}
