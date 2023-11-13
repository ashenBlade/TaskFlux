namespace TaskFlux.PriorityQueue.Heap;

/// <summary>
/// Структура записи в очереди.
/// Состоит из самого ключа, и очереди с полученными значениями
/// </summary>
internal readonly struct HeapRecord
{
    public HeapRecord(long key, byte[] initialRecord)
    {
        Key = key;
        Queue = new ChunkedQueue();
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
    public ChunkedQueue Queue { get; }

    public void Deconstruct(out long key, out ChunkedQueue queue)
    {
        key = Key;
        queue = Queue;
    }
}