namespace TaskFlux.Consensus.Tests.Stubs;

/// <summary>
/// Реализация фабрики таймеров, возвращающая один и тот же таймер
/// </summary>
public class ConstantTimerFactory(ITimer timer) : ITimerFactory
{
    public ITimer CreateHeartbeatTimer()
    {
        return timer;
    }

    public ITimer CreateElectionTimer()
    {
        return timer;
    }
}