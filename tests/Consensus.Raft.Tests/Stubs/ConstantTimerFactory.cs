namespace Consensus.Raft.Tests.Stubs;

/// <summary>
/// Реализация фабрики таймеров, возвращающая один и тот же таймер
/// </summary>
public class ConstantTimerFactory : ITimerFactory
{
    private readonly ITimer _timer;

    public ConstantTimerFactory(ITimer timer)
    {
        _timer = timer;
    }

    public ITimer CreateHeartbeatTimer()
    {
        return _timer;
    }

    public ITimer CreateElectionTimer()
    {
        return _timer;
    }
}