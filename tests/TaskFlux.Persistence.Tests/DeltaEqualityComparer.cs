using TaskFlux.Persistence.ApplicationState.Deltas;

namespace TaskFlux.Persistence.Tests;

public class DeltaEqualityComparer : IEqualityComparer<Delta>
{
    public static readonly DeltaEqualityComparer Instance = new();

    public bool Equals(Delta? x, Delta? y)
    {
        return Equal(( dynamic? ) x, ( dynamic? ) y);
    }

    private bool Equal(CreateQueueDelta left, CreateQueueDelta right)
    {
        return left.QueueName == right.QueueName
            && left.Code == right.Code
            && left.MaxMessageSize == right.MaxMessageSize
            && left.MaxQueueSize == right.MaxQueueSize
            && left.PriorityRange == right.PriorityRange;
    }

    private bool Equal(DeleteQueueDelta left, DeleteQueueDelta right)
    {
        return left.QueueName == right.QueueName;
    }

    private bool Equal(AddRecordDelta left, AddRecordDelta right)
    {
        return left.QueueName == right.QueueName && left.Key == right.Key && left.Message.SequenceEqual(right.Message);
    }

    private bool Equal(RemoveRecordDelta left, RemoveRecordDelta right)
    {
        return left.QueueName == right.QueueName && left.Key == right.Key && left.Message.SequenceEqual(right.Message);
    }

    public int GetHashCode(Delta obj)
    {
        return ( int ) obj.Type;
    }
}