using TaskFlux.Network.Responses;

namespace TaskFlux.Transport.Tcp.Common.Tests;

public class NetworkResponseEqualityComparer : IEqualityComparer<NetworkResponse>
{
    public static readonly NetworkResponseEqualityComparer Instance = new();

    public bool Equals(NetworkResponse? x, NetworkResponse? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (ReferenceEquals(x, null)) return false;
        if (ReferenceEquals(y, null)) return false;
        if (x.GetType() != y.GetType()) return false;
        return x.Type == y.Type && Check((dynamic)x, (dynamic)y);
    }

    private static bool Check(CountNetworkResponse first, CountNetworkResponse second)
    {
        return first.Count == second.Count;
    }

    private static bool Check(DequeueNetworkResponse first, DequeueNetworkResponse second)
    {
        return (first.Record is null && second.Record is null)
               || (first.Record is var (firstId, firstPriority, firstPayload)
                   && second.Record is var (secondId, secondPriority, secondPayload)
                   && firstId == secondId
                   && firstPriority == secondPriority
                   && firstPayload.SequenceEqual(secondPayload));
    }

    private static bool Check(ListQueuesNetworkResponse first, ListQueuesNetworkResponse second)
    {
        return first.Queues.SequenceEqual(second.Queues, TaskQueueInfoEqualityComparer);
    }

    private static readonly IEqualityComparer<ITaskQueueInfo> TaskQueueInfoEqualityComparer =
        new LambdaEqualityComparer<ITaskQueueInfo>(equals: (x, y) => x is not null
                                                                     && y is not null
                                                                     && x.QueueName == y.QueueName
                                                                     && x.Count == y.Count
                                                                     && x.Policies.ToHashSet(StringPairEqualityComparer)
                                                                         .SetEquals(y.Policies),
            hashCode: obj => HashCode.Combine(obj.QueueName, obj.Count, obj.Policies));

    private static readonly IEqualityComparer<KeyValuePair<string, string>> StringPairEqualityComparer =
        new LambdaEqualityComparer<KeyValuePair<string, string>>(equals: (x, y) => x.Key == y.Key && x.Value == y.Value,
            hashCode: x => HashCode.Combine(x.Key, x.Value));

    private static bool Check(PolicyViolationNetworkResponse first, PolicyViolationNetworkResponse second)
    {
        return NetworkPolicyEqualityComparer.Instance.Equals(first.ViolatedNetworkQueuePolicy,
            second.ViolatedNetworkQueuePolicy);
    }

    private static bool Check(ErrorNetworkResponse first, ErrorNetworkResponse second)
    {
        return first.ErrorType == second.ErrorType
               && first.Message == second.Message;
    }

    public int GetHashCode(NetworkResponse obj)
    {
        return (int)obj.Type;
    }
}