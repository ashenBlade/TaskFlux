namespace TaskFlux.PriorityQueue.Heap;

/// <summary>
/// Структура записи в очереди.
/// Состоит из самого ключа, и очереди с полученными значениями
/// </summary>
internal readonly struct HeapRecord<TData>
{
    public HeapRecord(long key, TData initialRecord)
    {
        Key = key;
        Queue = new ChunkedQueue<TData>();
        Queue.Enqueue(initialRecord);
    }

    /// <summary>
    /// Ключ записи
    /// </summary>
    public long Key { get; }

    /// <summary>
    /// Очередь с хранимыми значениями.
    /// Необходима для получения записей в порядке добавления.
    /// </summary>
    public ChunkedQueue<TData> Queue { get; }

    public void Deconstruct(out long key, out ChunkedQueue<TData> queue)
    {
        key = Key;
        queue = Queue;
    }
}