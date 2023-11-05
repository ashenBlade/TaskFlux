using TaskFlux.Core.Policies;
using TaskFlux.Core.Queue;

namespace TaskFlux.Core;

/// <summary>
/// Политика выполнения операций над очередью
/// </summary>
public abstract class QueuePolicy
{
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
    internal abstract bool CanEnqueue(long key, byte[] payload, IReadOnlyTaskQueue queue);

    /// <summary>
    /// Записать информацию о себе в переданную структуру метаданных.
    /// Политика должна обновить информацю о себе в метаданных
    /// </summary>
    /// <param name="metadata">Метаданные, в которые нужно записать информацию о себе</param>
    internal abstract void Enrich(TaskQueueMetadata metadata);

    public abstract TReturn Accept<TReturn>(IQueuePolicyVisitor<TReturn> visitor);
}