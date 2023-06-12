namespace Raft.Timers;

public class RandomizedSystemTimersTimer: SystemTimersTimer
{
    private readonly int _lowerDeltaMs;
    private readonly int _upperDeltaMs;
    
    public RandomizedSystemTimersTimer(TimeSpan lowerDelta, TimeSpan upperDelta, TimeSpan interval) : base(interval)
    {
        _lowerDeltaMs = (int) lowerDelta.TotalMilliseconds;
        _upperDeltaMs = (int) upperDelta.TotalMilliseconds;
    }

    public override void Start()
    {
        Timer.Interval = Interval.TotalMilliseconds + Random.Shared.Next(_lowerDeltaMs, _upperDeltaMs);
        base.Start();
    }
}