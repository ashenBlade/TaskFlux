using Consensus.CommandQueue;

namespace Consensus.Raft.Tests.Stubs;

public class SimpleCommandQueue : ICommandQueue
{
    public T Enqueue<T>(ICommand<T> command)
    {
        return command.Execute();
    }
}