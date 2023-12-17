using TaskFlux.Network.Responses.Policies;

namespace TaskFlux.Network.Packets.Tests;

public class NetworkPolicyEqualityComparer : IEqualityComparer<NetworkQueuePolicy>
{
    public static readonly NetworkPolicyEqualityComparer Instance = new();

    public bool Equals(NetworkQueuePolicy? x, NetworkQueuePolicy? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (ReferenceEquals(x, null)) return false;
        if (ReferenceEquals(y, null)) return false;
        if (x.GetType() != y.GetType()) return false;
        return x.Code == y.Code && Check(( dynamic ) x, ( dynamic ) y);
    }

    private static bool Check(GenericNetworkQueuePolicy x, GenericNetworkQueuePolicy y) =>
        x.Message == y.Message;

    private static bool Check(MaxMessageSizeNetworkQueuePolicy x, MaxMessageSizeNetworkQueuePolicy y) =>
        x.MaxMessageSize == y.MaxMessageSize;

    private static bool Check(MaxQueueSizeNetworkQueuePolicy x, MaxQueueSizeNetworkQueuePolicy y) =>
        x.MaxQueueSize == y.MaxQueueSize;

    private static bool Check(PriorityRangeNetworkQueuePolicy x, PriorityRangeNetworkQueuePolicy y) =>
        x.Min == y.Min && x.Max == y.Max;

    public int GetHashCode(NetworkQueuePolicy obj)
    {
        return ( int ) obj.Code;
    }
}