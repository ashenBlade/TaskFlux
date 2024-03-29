using TaskFlux.Core.Restore;

namespace TaskFlux.Persistence.Tests;

public class QueueInfoEqualityComparer : IEqualityComparer<QueueInfo>
{
    public static readonly QueueInfoEqualityComparer Instance = new();

    public bool Equals(QueueInfo? x, QueueInfo? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (ReferenceEquals(x, null)) return false;
        if (ReferenceEquals(y, null)) return false;
        if (x.GetType() != y.GetType()) return false;
        return x.QueueName.Equals(y.QueueName)
            && x.Code == y.Code
            && x.MaxQueueSize == y.MaxQueueSize
            && x.MaxPayloadSize == y.MaxPayloadSize
            && Nullable.Equals(x.PriorityRange, y.PriorityRange)
            && x.Data.ToHashSet().SetEquals(y.Data);
    }

    public int GetHashCode(QueueInfo obj)
    {
        return HashCode.Combine(obj.QueueName, ( int ) obj.Code, obj.MaxQueueSize, obj.MaxPayloadSize,
            obj.PriorityRange);
    }
}