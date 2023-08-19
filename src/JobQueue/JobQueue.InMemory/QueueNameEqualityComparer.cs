using JobQueue.Core;

namespace JobQueue.InMemory;

internal class QueueNameEqualityComparer: IEqualityComparer<QueueName>
{
    public static readonly QueueNameEqualityComparer Instance = new();
    public bool Equals(QueueName x, QueueName y)
    {
        return x.Name == y.Name;
    }

    public int GetHashCode(QueueName obj)
    {
        return obj.Name.GetHashCode();
    }
}