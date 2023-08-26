
using Consensus.CommandQueue;

namespace Consensus.Raft.Tests;

public class SimpleCommandQueue: ICommandQueue
{
    public T Enqueue<T>(ICommand<T> command)
    {
        return command.Execute();
    }
}