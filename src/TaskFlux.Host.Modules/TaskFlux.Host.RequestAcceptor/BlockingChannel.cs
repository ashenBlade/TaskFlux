using System.Collections.Concurrent;

namespace TaskFlux.Host.RequestAcceptor;

public class BlockingChannel<T> : IDisposable
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
        var buffer = new[] {token.WaitHandle, _signal};

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