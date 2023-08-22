namespace JobQueue.Core;

public interface IJobQueue
{
    /// <summary>
    /// Название текущей очереди
    /// </summary>
    public QueueName Name { get; }

    /// <summary>
    /// Количество элементов в ней на данный момент
    /// </summary>
    public uint Count { get; }

    /// <summary>
    /// Метаданные очереди
    /// </summary>
    public IJobQueueMetadata Metadata { get; }

    /// <summary>
    /// Добавить новый элемент в очередь
    /// </summary>
    /// <param name="key">Ключ</param>
    /// <param name="payload">Данные</param>
    /// <returns><c>true</c> - значение было добавлено в очередь, <c>false</c> - в очереди нет места для новых элементов</returns>
    public bool TryEnqueue(long key, byte[] payload);

    /// <summary>
    /// Получить элемент из очереди
    /// </summary>
    /// <param name="key">Полученный ключ</param>
    /// <param name="payload">Полученные данные</param>
    /// <returns><c>true</c> - элемент получен, <c>false</c> - очередь была пуста и ничего не получено</returns>
    /// <remarks>Если ничего не получено, то <paramref name="key"/> и <paramref name="payload"/> будут хранить <c>0</c> и <c>null</c></remarks>
    public bool TryDequeue(out long key, out byte[] payload);

    /// <summary>
    /// Получить список хранящихся в очереди данных в виде пары Приоритет/Нагрузка
    /// </summary>
    /// <returns>Набор хранящихся в очереди данных</returns>
    /// <remarks>
    /// Метод предназначен для сериализации.
    /// Список возвращаемых данных не обязан быть в правильном порядке
    /// </remarks>
    public IReadOnlyCollection<(long Priority, byte[] Payload)> GetAllData();
}