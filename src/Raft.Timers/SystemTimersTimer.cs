using System.Timers;
using Raft.Core;

namespace Raft.Timers;

public class SystemTimersTimer: ITimer, IDisposable
{
    private readonly System.Timers.Timer _timer; 
    public SystemTimersTimer(TimeSpan timeout)
    {
        _timer = new System.Timers.Timer()
        {
            AutoReset = false, 
            Enabled = false,
            Interval = timeout.TotalMilliseconds,
        };
        _timer.Elapsed += TimerOnElapsed;
    }

    public event Action? Timeout;
    private void OnTimeout()
    {
        Timeout?.Invoke();
    }

    private void TimerOnElapsed(object? sender, ElapsedEventArgs e)
    {
        OnTimeout();
    }


    public void Start()
    {
        _timer.Start();
    }

    public void Reset()
    {
        _timer.Stop();
        _timer.Start();
    }

    public void Stop()
    {
        _timer.Stop();
    }

    public void Dispose()
    {
        _timer.Elapsed -= TimerOnElapsed;
        _timer.Dispose();
    }
}