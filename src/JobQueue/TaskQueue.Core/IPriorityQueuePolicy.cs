namespace TaskQueue.Core;

internal interface IPriorityQueuePolicy
{
    /// <summary>
    /// Сделать проверку на возможность добавления записи в очередь
    /// </summary>
    /// <param name="key">Добавляемый ключ</param>
    /// <param name="payload">Добавляемые данные</param>
    /// <param name="queue">Очередь, для которой нужно сделать проверку</param>
    /// <param name="error">Возникшая ошибка, если нельзя добавлять</param>
    /// <returns>
    /// <c>true</c> - проверка прошла успешно,
    /// <c>false</c> - добавлять нельзя (<paramref name="error"/> содержит объект ошибки)
    /// </returns>
    public bool CanEnqueue(long key, byte[] payload, ITaskQueue queue, out EnqueueResult error);

    /// <summary>
    /// Записать информацию о себе в переданную структуру метаданных.
    /// Политика должна обновить информацю о себе в метаданных
    /// </summary>
    /// <param name="metadata">Метаданные, в которые нужно записать информацию о себе</param>
    public void Enrich(TaskQueueMetadata metadata);
}