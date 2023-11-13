namespace TaskFlux.PriorityQueue;

/// <summary>
/// Интерфейс структуры приоритетной очереди.
/// Является бэкэндом для логики хранения записей очереди в памяти
/// </summary>
public interface IPriorityQueue
{
    /// <summary>
    /// Код, используемый очередью
    /// </summary>
    public PriorityQueueCode Code { get; }

    /// <summary>
    /// Получить текущее количество элементов в очереди
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Добавить новый элемент в очередь
    /// </summary>
    /// <param name="key">Ключ записи, приоритет</param>
    /// <param name="payload">Значение, которое нужно сохранить</param>
    public void Enqueue(long key, byte[] payload);

    /// <summary>
    /// Попытаться прочитать запись <paramref name="payload"/> с ключом <paramref name="key"/> из очереди 
    /// </summary>
    /// <param name="key">Полученный ключ записи, приоритет</param>
    /// <param name="payload">Полученное значение этой записи</param>
    /// <returns>
    /// <c>true</c> - запись была прочитана,
    /// <c>false</c> - очередь была пуста и прочитать ее не удалось
    /// </returns>
    public bool TryDequeue(out long key, out byte[] payload);

    /// <summary>
    /// Прочитать все записи, хранящиеся в очереди
    /// </summary>
    /// <returns>Набор из пар (ключ, значение), которое хранилось в очереди</returns>
    /// <remarks>Полученная коллекция не обязательно будет сортирована в порядке приоритета</remarks>
    IReadOnlyCollection<(long Priority, byte[] Payload)> ReadAllData();
}