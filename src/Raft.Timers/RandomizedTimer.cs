using System.Timers;
using Raft.Core;
using Timer = System.Timers.Timer;

namespace Raft.Timers;

public class RandomizedTimer: ITimer, IDisposable
{
    private readonly TimeSpan _lower;
    private readonly TimeSpan _upper;
    private Timer? _timer;

    public RandomizedTimer(TimeSpan lower, TimeSpan upper)
    {
        _lower = lower;
        _upper = upper;
        _timer = null;
    }
    private double CalculateRandomInterval()
    {
        return Random.Shared.Next(( int ) _lower.TotalMilliseconds, ( int ) _upper.TotalMilliseconds);
    }

    private Timer CreateTimer()
    {
        var timer = new Timer() {Interval = CalculateRandomInterval(), Enabled = true, AutoReset = false};
        timer.Elapsed += TimerOnElapsed; 
        return timer;
    }

    private void TimerOnElapsed(object? sender, ElapsedEventArgs e)
    {
        OnTimeout();
    }

    public void Start()
    {
        if (_timer is not null)
        {
            _timer.Dispose();
            _timer = null;
        }
        var timer = CreateTimer();
        timer.Enabled = true;
        _timer = timer;
        _timer.Enabled = true;
    }

    public void Reset()
    {
        if (_timer is not null)
        {
            _timer.Dispose();
            _timer = null;
        }
        var timer = CreateTimer();
        timer.Enabled = true;
        _timer = timer;
        _timer.Enabled = true;
    }

    public void Stop()
    {
        if (_timer is not null)
        {
            _timer.Dispose();
            _timer = null;
        }
    }

    public event Action? Timeout;

    public void Dispose()
    {
        _timer?.Dispose();
    }

    protected virtual void OnTimeout()
    {
        Timeout?.Invoke();
    }
}