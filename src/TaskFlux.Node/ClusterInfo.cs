using System.Net;
using TaskFlux.Core;

namespace TaskFlux.Node;

public class ClusterInfo : IClusterInfo
{
    public ClusterInfo(NodeId? leaderId, NodeId nodeId, IEnumerable<EndPoint> nodes)
    {
        LeaderId = leaderId;
        CurrentNodeId = nodeId;
        Nodes = nodes.ToArray();
    }

    public NodeId? LeaderId { get; set; }
    public IReadOnlyList<EndPoint> Nodes { get; }
    public NodeId CurrentNodeId { get; }

    public override string ToString()
    {
        return $"ClusterInfo(LeaderId = {LeaderId})";
    }
}