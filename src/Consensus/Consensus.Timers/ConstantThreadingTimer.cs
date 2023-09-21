namespace Consensus.Timers;

internal class ConstantThreadingTimer : ThreadingTimer
{
    private readonly int _sleepTimeMs;

    public ConstantThreadingTimer(TimeSpan timeout)
    {
        _sleepTimeMs = ( int ) timeout.TotalMilliseconds;
    }

    protected override int GetSleepTimeMs()
    {
        return _sleepTimeMs;
    }
}