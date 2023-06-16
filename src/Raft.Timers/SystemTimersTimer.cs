using System.Timers;
using Raft.Core;

namespace Raft.Timers;

public class SystemTimersTimer: ITimer, IDisposable
{
    protected readonly System.Timers.Timer Timer;
    protected readonly TimeSpan Interval;
    public SystemTimersTimer(TimeSpan timeout)
    {
        Timer = new System.Timers.Timer()
        {
            AutoReset = false, 
            Enabled = false,
            Interval = timeout.TotalMilliseconds,
        };
        Interval = timeout;
        Timer.Elapsed += TimerOnElapsed;
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


    public virtual void Start()
    {
        Timer.Start();
    }

    public virtual void Reset()
    {
        Timer.Stop();
        Timer.Start();
    }

    public void Stop()
    {
        Timer.Stop();
    }

    public void Dispose()
    {
        Timer.Elapsed -= TimerOnElapsed;
        Timer.Dispose();
    }
}