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
    /// <remarks>Для правильной работы <paramref name="lower"/> должен быть больше <paramref name="heartbeatTimeout"/> </remarks>
    public ThreadingTimerFactory(TimeSpan lower, TimeSpan upper, TimeSpan heartbeatTimeout)
    {
        _lower = lower;
        _upper = upper;
        _heartbeatTimeout = heartbeatTimeout;
    }

    public Raft.ITimer CreateHeartbeatTimer()
    {
        return new ConstantThreadingTimer(_heartbeatTimeout);
    }

    public Raft.ITimer CreateElectionTimer()
    {
        return new RandomizedThreadingTimer(_lower, _upper);
    }
}