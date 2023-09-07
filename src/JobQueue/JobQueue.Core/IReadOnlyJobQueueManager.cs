namespace JobQueue.Core;

public interface IReadOnlyJobQueueManager
{
    /// <summary>
    /// Проверить, что очередь с указанным названием существует
    /// </summary>
    /// <param name="name">Название очереди</param>
    /// <returns><c>true</c> - очередь существует, <c>false</c> - очередь не существует</returns>
    public bool HasQueue(QueueName name);

    /// <summary>
    /// Получить метаданные обо всех очередях, находящихся в системе
    /// </summary>
    /// <returns>Список из метаданных очередей</returns>
    public IReadOnlyCollection<IJobQueueMetadata> GetAllQueuesMetadata();

    /// <summary>
    /// Количество очередей, которыми <see cref="IJobQueueManager"/> управляет
    /// </summary>
    public int QueuesCount { get; }

    /// <summary>
    /// Получить список из всех очередей, присутствующих в системе.
    /// </summary>
    /// <remarks>
    /// Необходимо для создания снапшота.
    /// Для работы лучше пользоваться другими методами.
    /// </remarks>
    /// <returns>Список из всех очередей</returns>
    public IEnumerable<IReadOnlyJobQueue> GetAllQueues();

    /// <summary>
    /// Получить именованную очередь с приоритетом
    /// </summary>
    /// <param name="name">Название очереди с приоритетом</param>
    /// <param name="jobQueue">Найденная очередь задач, если нашлась, иначе <c>null</c></param>
    /// <returns><c>true</c> - очередь с таким названием нашлась, <c>false</c> - очередь с таким названием не нашлась</returns>
    /// <remarks>Если очередь не нашлась, то <paramref name="jobQueue"/> - <c>null</c></remarks>
    public bool TryGetQueue(QueueName name, out IReadOnlyJobQueue jobQueue);
}