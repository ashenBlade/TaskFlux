namespace TaskFlux.Abstractions;

public interface IPriorityQueue<TKey, TValue>
{
    /// <summary>
    /// Получить текущее количество элементов в очереди
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Добавить новый элемент в очередь
    /// </summary>
    /// <param name="key">Ключ записи, приоритет</param>
    /// <param name="value">Значение, которое нужно сохранить</param>
    public void Enqueue(TKey key, TValue value);

    /// <summary>
    /// Попытаться прочитать запись <paramref name="value"/> с ключом <paramref name="key"/> из очереди 
    /// </summary>
    /// <param name="key">Полученный ключ записи, приоритет</param>
    /// <param name="value">Полученное значение этой записи</param>
    /// <returns>
    /// <c>true</c> - запись была прочитана,
    /// <c>false</c> - очередь была пуста и прочитать ее не удалось
    /// </returns>
    public bool TryDequeue(out TKey key, out TValue value);

    /// <summary>
    /// Прочитать все записи, хранящиеся в очереди
    /// </summary>
    /// <returns>Набор из пар (ключ, значение), которое хранилось в очереди</returns>
    /// <remarks>Полученная коллекция не обязательно будет сортирована в порядке приоритета</remarks>
    IReadOnlyCollection<(TKey Priority, TValue Payload)> ReadAllData();
}