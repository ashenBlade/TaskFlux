namespace TaskFlux.Core;

public class QueueNameEqualityComparer : IEqualityComparer<QueueName>
{
    public static readonly QueueNameEqualityComparer Instance = new();

    public bool Equals(QueueName x, QueueName y)
    {
        return x == y;
    }

    public int GetHashCode(QueueName obj)
    {
        return obj.GetHashCode();
    }
}