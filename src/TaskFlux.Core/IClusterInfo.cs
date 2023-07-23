namespace TaskFlux.Core;

public interface IClusterInfo
{
    public NodeId LeaderId { get; }
    public int ClusterSize { get; }
}