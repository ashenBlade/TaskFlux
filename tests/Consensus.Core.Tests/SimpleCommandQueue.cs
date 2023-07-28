
using Consensus.CommandQueue;

namespace Consensus.Core.Tests;

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