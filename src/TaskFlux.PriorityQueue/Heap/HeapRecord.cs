namespace TaskFlux.PriorityQueue.Heap;

internal class HeapRecord
{
    public HeapRecord(long key, byte[] initialRecord)
    {
        Key = key;
        Queue = new ChunkedQueue();
        Queue.Enqueue(initialRecord);
    }

    public long Key { get; }
    public ChunkedQueue Queue { get; }

    public void Deconstruct(out long key, out ChunkedQueue queue)
    {
        key = Key;
        queue = Queue;
    }
}