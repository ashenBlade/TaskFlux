namespace JobQueue.Core;

/// <summary>
/// Менеджер для управления очередями задач
/// </summary>
public interface IJobQueueManager
{
    /// <summary>
    /// Получить именованную очередь с приоритетом
    /// </summary>
    /// <param name="name">Название очереди с приоритетом</param>
    /// <param name="jobQueue">Найденная очередь задач, если нашлась, иначе <c>null</c></param>
    /// <returns><c>true</c> - очередь с таким названием нашлась, <c>false</c> - очередь с таким названием не нашлась</returns>
    /// <remarks>Если очередь не нашлась, то <paramref name="jobQueue"/> - <c>null</c></remarks>
    public bool TryGetQueue(QueueName name, out IJobQueue jobQueue);

    /// <summary>
    /// Добавить очередь с приоритетом в список отслеживаемых менеджером
    /// </summary>
    /// <param name="name">Название очереди</param>
    /// <param name="jobQueue">Очередь с приоритетом</param>
    /// <returns><c>true</c> - очередь добавлена, <c>false</c> - очередь не добавлена из-за уже существующей очереди с таким-же названием</returns>
    /// <exception cref="ArgumentNullException"><paramref name="jobQueue"/> - <c>null</c></exception>
    public bool TryAddQueue(QueueName name, IJobQueue jobQueue);

    /// <summary>
    /// Удалить очередь с указанным названием
    /// </summary>
    /// <param name="name">Название очереди с приоритетом</param>
    /// <param name="deleted">Удаленная очередь</param>
    /// <returns><c>true</c> - очередь была удалена, <c>false</c> - очередь с указанным названием не найдена</returns>
    /// <remarks>Если </remarks>
    public bool TryDeleteQueue(QueueName name, out IJobQueue deleted);

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
    public IEnumerable<IJobQueue> GetAllQueues();
}