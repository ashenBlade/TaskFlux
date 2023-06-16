using Raft.Core.Commands;

namespace Raft.CommandQueue.Semaphore;

public class SemaphoreCommandQueue: ICommandQueue, IDisposable
{
    private readonly SemaphoreSlim _sem = new(0, 1);

    public void Enqueue(ICommand command)
    {
        _sem.Wait();
        try
        {
            command.Execute();
        }
        finally
        {
            _sem.Release();
        }
    }

    public T Enqueue<T>(ICommand<T> command)
    {
        _sem.Wait();
        try
        {
            return command.Execute();
        }
        finally
        {
            _sem.Release();
        }
    }

    public void Start()
    {
        _sem.Release();
    }

    public void Dispose()
    {
        _sem.Dispose();
    }
}