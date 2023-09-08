using Consensus.Raft;

namespace Consensus.Timers;

public class ThreadingTimerFactory : ITimerFactory
{
    private readonly TimeSpan _lower;
    private readonly TimeSpan _upper;
    private readonly TimeSpan _heartbeatTimeout;

    /// <summary>
    /// Основной конструктор для фабрики таймеров,
    /// испльзующих <see cref="System.Threading.Timer"/>
    /// </summary>
    /// <param name="lower">Нижняя граница интервала таймера выборов</param>
    /// <param name="upper">Верхняя граница интервала таймера выборов</param>
    /// <param name="heartbeatTimeout">Таймаут для Heartbeat запросов</param>
    public ThreadingTimerFactory(TimeSpan lower, TimeSpan upper, TimeSpan heartbeatTimeout)
    {
        _lower = lower;
        _upper = upper;
        _heartbeatTimeout = heartbeatTimeout;
    }

    public ITimer CreateHeartbeatTimer()
    {
        return new ConstantThreadingTimer(_heartbeatTimeout);
    }

    public ITimer CreateElectionTimer()
    {
        return new RandomizedThreadingTimer(_lower, _upper);
    }
}