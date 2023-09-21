using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("TaskFlux.Host.RequestAcceptor.Tests")]

namespace TaskFlux.Host.RequestAcceptor;

internal class BlockingChannel<T> : IDisposable
{
    private readonly ConcurrentQueue<T> _queue = new();
    private readonly AutoResetEvent _signal = new(false);

    public void Write(T item)
    {
        _queue.Enqueue(item);
        _signal.Set();
    }

    public IEnumerable<T> ReadAll(CancellationToken token = default)
    {
        WaitHandle[] buffer;
        try
        {
            buffer = new[] {token.WaitHandle, _signal};
        }
        catch (ObjectDisposedException)
        {
            yield break;
        }

        while (!token.IsCancellationRequested)
        {
            try
            {
                var index = WaitHandle.WaitAny(buffer);
                if (index == 0)
                {
                    break;
                }
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            while (_queue.TryDequeue(out var item))
            {
                yield return item;
            }
        }

        while (_queue.TryDequeue(out var item))
        {
            yield return item;
        }
    }

    public void Dispose()
    {
        _signal.Dispose();
    }
}