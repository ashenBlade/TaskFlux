using TaskFlux.Network.Packets.Commands;

namespace TaskFlux.Network.Packets.Tests;

[Trait("Category", "Serialization")]
public class NetworkCommandEqualityComparer : IEqualityComparer<NetworkCommand>
{
    public static readonly NetworkCommandEqualityComparer Instance = new();

    public bool Equals(NetworkCommand? x, NetworkCommand? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (ReferenceEquals(x, null)) return false;
        if (ReferenceEquals(y, null)) return false;
        if (x.GetType() != y.GetType()) return false;
        return x.Type == y.Type && Check(( dynamic ) x, ( dynamic ) y);
    }

    private static bool Check(CountNetworkCommand left, CountNetworkCommand right) => left.QueueName == right.QueueName;

    private static bool Check(CreateQueueNetworkCommand left, CreateQueueNetworkCommand right) =>
        left.QueueName == right.QueueName
     && left.MaxMessageSize == right.MaxMessageSize
     && left.MaxQueueSize == right.MaxQueueSize
     && left.Code == right.Code
     && left.PriorityRange == right.PriorityRange;

    private static bool Check(DeleteQueueNetworkCommand left, DeleteQueueNetworkCommand right) =>
        left.QueueName == right.QueueName;

    private static bool Check(DequeueNetworkCommand left, DequeueNetworkCommand right) =>
        left.QueueName == right.QueueName;

    private static bool Check(EnqueueNetworkCommand left, EnqueueNetworkCommand right) =>
        left.QueueName == right.QueueName
     && left.Key == right.Key
     && left.Message.SequenceEqual(right.Message);

    private static bool Check(ListQueuesNetworkCommand left, ListQueuesNetworkCommand right) => true;

    public int GetHashCode(NetworkCommand obj)
    {
        return ( int ) obj.Type;
    }
}