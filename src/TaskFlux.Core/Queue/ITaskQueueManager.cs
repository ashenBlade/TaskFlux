namespace TaskFlux.Core.Queue;

/// <summary>
/// Менеджер для управления очередями задач
/// </summary>
public interface ITaskQueueManager : IReadOnlyTaskQueueManager
{
    /// <summary>
    /// Получить именованную очередь с приоритетом
    /// </summary>
    /// <param name="name">Название очереди с приоритетом</param>
    /// <param name="taskQueue">Найденная очередь задач, если нашлась, иначе <c>null</c></param>
    /// <returns><c>true</c> - очередь с таким названием нашлась, <c>false</c> - очередь с таким названием не нашлась</returns>
    /// <remarks>Если очередь не нашлась, то <paramref name="taskQueue"/> - <c>null</c></remarks>
    public bool TryGetQueue(QueueName name, out ITaskQueue taskQueue);

    /// <summary>
    /// Добавить очередь с приоритетом в список отслеживаемых менеджером
    /// </summary>
    /// <param name="name">Название очереди</param>
    /// <param name="taskQueue">Очередь с приоритетом</param>
    /// <returns><c>true</c> - очередь добавлена, <c>false</c> - очередь не добавлена из-за уже существующей очереди с таким-же названием</returns>
    /// <exception cref="ArgumentNullException"><paramref name="taskQueue"/> - <c>null</c></exception>
    public bool TryAddQueue(QueueName name, ITaskQueue taskQueue);

    /// <summary>
    /// Удалить очередь с указанным названием
    /// </summary>
    /// <param name="name">Название очереди с приоритетом</param>
    /// <param name="deleted">Удаленная очередь</param>
    /// <returns><c>true</c> - очередь была удалена, <c>false</c> - очередь с указанным названием не найдена</returns>
    /// <remarks>Если </remarks>
    public bool TryDeleteQueue(QueueName name, out ITaskQueue deleted);

    /// <summary>
    /// Получить список из всех очередей, присутствующих в системе.
    /// </summary>
    /// <remarks>
    /// Необходимо для создания снапшота.
    /// Для работы лучше пользоваться другими методами.
    /// </remarks>
    /// <returns>Список из всех очередей</returns>
    public new IReadOnlyCollection<ITaskQueue> GetAllQueues();
}