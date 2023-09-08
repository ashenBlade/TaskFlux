namespace Consensus.Raft;

public interface ITimer : IDisposable
{
    /// <summary>
    /// Принудительно запустить таймер.
    /// После вызова, сразу срабатывает событие <see cref="Timeout"/>
    /// </summary>
    void ForceRun();

    /// <summary>
    /// Запустить таймер на срабатывание после таймаута
    /// </summary>
    void Schedule();

    /// <summary>
    /// Принудительно остановить таймер
    /// </summary>
    void Stop();

    /// <summary>
    /// Событие, срабатывающее, когда таймер сработал (истек таймаут)
    /// </summary>
    event Action Timeout;
}