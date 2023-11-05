using TaskFlux.Core;

namespace TaskQueue.Core.Policies;

/// <summary>
/// Политика выполнения операций над очередью
/// </summary>
public abstract class QueuePolicy
{
    // TODO: код и сообщение об ошибке (Message) абстрактные

    /// <summary>
    /// Сделать проверку на возможность добавления записи в очередь
    /// </summary>
    /// <param name="key">Добавляемый ключ</param>
    /// <param name="payload">Добавляемые данные</param>
    /// <param name="queue">Очередь, для которой нужно сделать проверку</param>
    /// <returns>
    /// <c>true</c> - проверка прошла успешно,
    /// <c>false</c> - добавлять нельзя
    /// </returns>
    public abstract bool CanEnqueue(long key, byte[] payload, IReadOnlyTaskQueue queue);

    /// <summary>
    /// Записать информацию о себе в переданную структуру метаданных.
    /// Политика должна обновить информацю о себе в метаданных
    /// </summary>
    /// <param name="metadata">Метаданные, в которые нужно записать информацию о себе</param>
    public abstract void Enrich(TaskQueueMetadata metadata);
}