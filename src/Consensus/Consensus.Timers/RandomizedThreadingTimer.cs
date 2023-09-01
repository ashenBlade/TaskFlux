using System.Diagnostics;
using Consensus.Raft;

namespace Consensus.Timers;

public class RandomizedThreadingTimer : ITimer
{
    private const int Infinite = System.Threading.Timeout.Infinite;
    private readonly TimeSpan _lower;
    private readonly TimeSpan _upper;
    private readonly Timer _timer;
    private readonly Random _random;
    public event Action? Timeout;

    public RandomizedThreadingTimer(TimeSpan lower, TimeSpan upper)
    {
        _lower = lower;
        _upper = upper;
        _timer = new Timer(OnTimeout);
        // Не думаю, что нужен сид
        // Под капотом, он все равно задается случайным образом
        _random = new();
    }

    public void Start()
    {
        var sleepTime = GetNextSleepTime();
        var success = _timer.Change(sleepTime, Infinite);
        Debug.Assert(success, "Таймер не удалось настроить");
    }

    public void Stop()
    {
        _timer.Change(Infinite, Infinite);
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
        _timer.Dispose();
    }
}