namespace JobQueue.Core;

public interface IJobQueue
{
    public bool TryEnqueue(int key, byte[] payload);
    public bool TryDequeue(out int key, out byte[] payload);
    public int Count { get; }
}