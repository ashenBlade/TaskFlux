using System.Diagnostics.CodeAnalysis;
using TaskFlux.Network.Packets.Responses;
using TestHelpers;

namespace TaskFlux.Network.Packets.Tests;

public class NetworkResponseEqualityComparer : IEqualityComparer<NetworkResponse>
{
    public static readonly NetworkResponseEqualityComparer Instance = new();

    public bool Equals(NetworkResponse? x, NetworkResponse? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (ReferenceEquals(x, null)) return false;
        if (ReferenceEquals(y, null)) return false;
        if (x.GetType() != y.GetType()) return false;
        return x.Type == y.Type && Check(( dynamic ) x, ( dynamic ) y);
    }

    private static bool Check(CountNetworkResponse first, CountNetworkResponse second)
    {
        return first.Count == second.Count;
    }

    private static bool Check(DequeueNetworkResponse first, DequeueNetworkResponse second)
    {
        return (
                   first.Data is null && second.Data is null
               )
            || (
                   first.Data is var (firstKey, firstData)
                && second.Data is var (secondKey, secondData)
                && firstKey == secondKey
                && firstData.SequenceEqual(secondData)
               );
    }

    private static bool Check(ListQueuesNetworkResponse first, ListQueuesNetworkResponse second)
    {
        return first.Queues.SequenceEqual(second.Queues, TaskQueueInfoEqualityComparer);
    }

    private static readonly IEqualityComparer<ITaskQueueInfo> TaskQueueInfoEqualityComparer =
        new LambdaEqualityComparer<ITaskQueueInfo>(
            equals: (x, y) => x is not null
                           && y is not null
                           && x.QueueName == y.QueueName
                           && x.Count == y.Count
                           && x.Policies.ToHashSet(StringPairEqualityComparer).SetEquals(y.Policies),
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
        return first.ErrorType == second.ErrorType && first.Message == second.Message;
    }

    [SuppressMessage("ReSharper", "UnusedParameter.Local")]
    private static bool Check(OkNetworkResponse _, OkNetworkResponse __) => true;

    public int GetHashCode(NetworkResponse obj)
    {
        return ( int ) obj.Type;
    }
}