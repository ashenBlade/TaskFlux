using TaskFlux.Core.Queue;

namespace TaskFlux.Core.Subscription;

/// <summary>
/// Интерфейс подписчика на запись из очереди.
/// Используется для блокирующей ожидающей операции чтения.
/// Этот интерфейс реализует <see cref="IDisposable"/> - его обязательно нужно вызывать, лучше с помощью using конструкции
/// </summary>
public interface IQueueSubscriber : IDisposable
{
    /// <summary>
    /// Начать ожидание записи из очереди с учетом таймаута
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Прочитанная запись</returns>
    public ValueTask<QueueRecord> WaitRecordAsync(CancellationToken token = default);
}