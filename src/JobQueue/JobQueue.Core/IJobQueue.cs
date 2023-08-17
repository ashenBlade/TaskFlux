namespace JobQueue.Core;

public interface IJobQueue
{
    public bool TryEnqueue(long key, byte[] payload);
    public bool TryDequeue(out long key, out byte[] payload);
    public int Count { get; }
}