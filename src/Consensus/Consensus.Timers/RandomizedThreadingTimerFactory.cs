using Consensus.Raft;

namespace Consensus.Timers;

public class RandomizedThreadingTimerFactory : ITimerFactory
{
    private readonly TimeSpan _lower;
    private readonly TimeSpan _upper;

    public RandomizedThreadingTimerFactory(TimeSpan lower, TimeSpan upper)
    {
        _lower = lower;
        _upper = upper;
    }

    public ITimer CreateTimer()
    {
        return new RandomizedThreadingTimer(_lower, _upper);
    }
}