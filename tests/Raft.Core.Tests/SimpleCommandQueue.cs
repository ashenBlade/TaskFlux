using System.Threading.Channels;
using System.Windows.Input;
using Raft.CommandQueue;
using Raft.Core.Commands;
using ICommand = Raft.Core.Commands.ICommand;

namespace Raft.Core.Tests;

public class SimpleCommandQueue: ICommandQueue
{
    public void Enqueue(ICommand command)
    {
        command.Execute();
    }

    public T Enqueue<T>(ICommand<T> command)
    {
        return command.Execute();
    }
}