namespace Consensus.Raft;

public interface ITimerFactory
{
    /// <summary>
    /// Метод для создания нового таймера для Heartbeat.
    /// Предназначен для лидера при создании обработчиков сторонних узлов
    /// </summary>
    /// <returns>Новый таймер</returns>
    /// <remarks>Для создаваемых объектов нужно вызывать <see cref="IDisposable.Dispose"/> самим</remarks>
    public ITimer CreateHeartbeatTimer();

    /// <summary>
    /// Создаить новый таймер выборов.
    /// Используется кандидатом и последователем при инициализации для отслеживания
    /// падения лидера.
    /// </summary>
    /// <returns>Новый таймер выборов</returns>
    public ITimer CreateElectionTimer();
}