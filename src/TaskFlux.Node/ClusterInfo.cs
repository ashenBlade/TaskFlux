using TaskFlux.Core;

namespace TaskFlux.Node;

public class ClusterInfo: IClusterInfo
{
    public ClusterInfo(NodeId leaderId, int clusterSize)
    {
        LeaderId = leaderId;
        ClusterSize = clusterSize;
    }
    public NodeId LeaderId { get; set; }
    public int ClusterSize { get; }

    public override string ToString()
    {
        return $"ClusterInfo(LeaderId = {LeaderId.Value}, ClusterSize = {ClusterSize})";
    }
}