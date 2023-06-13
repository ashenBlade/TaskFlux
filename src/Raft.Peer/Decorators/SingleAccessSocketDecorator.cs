namespace Raft.Peer;

public class SingleAccessSocketDecorator: ISocket, IDisposable
{
    private readonly ISocket _socketImplementation;
    private readonly SemaphoreSlim _semaphoreSlim = new(1);

    public SingleAccessSocketDecorator(ISocket socketImplementation)
    {
        _socketImplementation = socketImplementation;
    }

    public void Dispose()
    {
        _socketImplementation.Dispose();
        _semaphoreSlim.Dispose();
    }

    public async Task SendAsync(ReadOnlyMemory<byte> payload, CancellationToken token = default)
    {
        await _semaphoreSlim.WaitAsync(token);
        try
        {
            await _socketImplementation.SendAsync(payload, token);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public async ValueTask ReadAsync(Stream stream, CancellationToken token = default)
    {
        await _semaphoreSlim.WaitAsync(token);
        try
        {
            await _socketImplementation.ReadAsync(stream, token);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }
}