using TaskFlux.Core.Policies;

namespace TaskFlux.Core.Tests;

public class QueuePolicyEqualityComparer : IEqualityComparer<QueuePolicy>
{
    public static readonly QueuePolicyEqualityComparer Comparer = new();

    public bool Equals(QueuePolicy? x, QueuePolicy? y)
    {
        return Check(( dynamic ) x!, ( dynamic ) y!);
    }

    private bool Check(MaxQueueSizeQueuePolicy left, MaxQueueSizeQueuePolicy right) =>
        left.MaxQueueSize == right.MaxQueueSize;

    private bool Check(MaxPayloadSizeQueuePolicy left, MaxPayloadSizeQueuePolicy right) =>
        left.MaxPayloadSize == right.MaxPayloadSize;

    private bool Check(PriorityRangeQueuePolicy left, PriorityRangeQueuePolicy right) =>
        ( left.Min, left.Max ) == ( right.Min, right.Max );

    public int GetHashCode(QueuePolicy obj)
    {
        return obj.GetHashCode();
    }
}