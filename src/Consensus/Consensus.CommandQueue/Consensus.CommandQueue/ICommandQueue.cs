namespace Consensus.CommandQueue;

public interface ICommandQueue
{
    public T Enqueue<T>(ICommand<T> command);
}