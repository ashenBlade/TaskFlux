namespace TaskFlux.Consensus.Timers;

internal class RandomizedThreadingTimer : ThreadingTimer
{
    private readonly TimeSpan _lower;
    private readonly TimeSpan _upper;
    private readonly Random _random;

    public RandomizedThreadingTimer(TimeSpan lower, TimeSpan upper)
    {
        _lower = lower;
        _upper = upper;
        _random = new();
    }

    protected override int GetSleepTimeMs()
    {
        var lowerMs = ( int ) _lower.TotalMilliseconds;
        var upperMs = ( int ) _upper.TotalMilliseconds;
        return _random.Next(lowerMs, upperMs);
    }
}