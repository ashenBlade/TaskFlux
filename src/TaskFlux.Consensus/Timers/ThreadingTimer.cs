namespace TaskFlux.Consensus.Timers;

internal abstract class ThreadingTimer : ITimer
{
    private const int Infinite = System.Threading.Timeout.Infinite;

    private volatile bool _disposed;
    private readonly Timer _timer;
    public event Action? Timeout;

    protected ThreadingTimer()
    {
        _timer = new Timer(OnTimeout);
    }

    /// <summary>
    /// Вычислить время сна в миллисекундах при вызове <see cref="Schedule"/>
    /// </summary>
    /// <returns>Время сна в миллисекундах</returns>
    protected abstract int GetSleepTimeMs();

    public void ForceRun()
    {
        if (_disposed)
        {
            return;
        }

        _timer.Change(0, Infinite);
    }

    public void Schedule()
    {
        if (_disposed)
        {
            return;
        }

        var sleepTime = GetSleepTimeMs();

        try
        {
            _timer.Change(sleepTime, Infinite);
        }
        catch (ObjectDisposedException)
        {
        }
    }

    public void Stop()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            _timer.Change(Infinite, Infinite);
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private void OnTimeout(object? state)
    {
        Timeout?.Invoke();
    }

    public void Dispose()
    {
        _disposed = true;
        _timer.Dispose();
    }
}