namespace TaskFlux.Delta.Tests;

public class DeltaEqualityComparer : IEqualityComparer<Delta>
{
    public static readonly DeltaEqualityComparer Instance = new();

    public bool Equals(Delta? x, Delta? y)
    {
        return Equal(( dynamic? ) x, ( dynamic? ) y);
    }

    public bool Equal(CreateQueueDelta left, CreateQueueDelta right)
    {
        return left.QueueName == right.QueueName
            && left.ImplementationType == right.ImplementationType
            && left.MaxMessageSize == right.MaxMessageSize
            && left.MaxQueueSize == right.MaxQueueSize
            && left.PriorityRange == right.PriorityRange;
    }

    public bool Equal(DeleteQueueDelta left, DeleteQueueDelta right)
    {
        return left.QueueName == right.QueueName;
    }

    public bool Equal(AddRecordDelta left, AddRecordDelta right)
    {
        return left.QueueName == right.QueueName && left.Key == right.Key && left.Message.SequenceEqual(right.Message);
    }

    public bool Equal(RemoveRecordDelta left, RemoveRecordDelta right)
    {
        return left.QueueName == right.QueueName && left.Key == right.Key && left.Message.SequenceEqual(right.Message);
    }

    public int GetHashCode(Delta obj)
    {
        return ( int ) obj.Type;
    }
}