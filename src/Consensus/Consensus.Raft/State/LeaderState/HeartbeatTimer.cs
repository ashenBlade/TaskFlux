using System.Diagnostics;

namespace Consensus.Raft.State.LeaderState;

public class HeartbeatTimer : IDisposable
{
    private readonly TimeSpan _lower;
    private readonly TimeSpan _upper;
    private readonly Timer _timer;
    private readonly Random _random;

    public HeartbeatTimer(TimeSpan lower, TimeSpan upper)
    {
        _lower = lower;
        _upper = upper;
        _random = new();
        _timer = new Timer(OnTimeout);
    }

    public void Start()
    {
        var sleepTime = GetNextSleepTime();
        var success = _timer.Change(sleepTime, System.Threading.Timeout.Infinite);
        Debug.Assert(success, "Таймер не удалось настроить");
    }

    private void OnTimeout(object? state)
    {
        Timeout?.Invoke();
    }

    private int GetNextSleepTime()
    {
        var lowerMs = ( int ) _lower.TotalMilliseconds;
        var upperMs = ( int ) _upper.TotalMilliseconds;
        return _random.Next(lowerMs, upperMs);
    }

    public void Dispose()
    {
        // _timer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);
        _timer.Dispose();
    }

    public event Action? Timeout;
}