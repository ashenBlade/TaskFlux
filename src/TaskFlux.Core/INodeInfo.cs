namespace TaskFlux.Core;

public interface INodeInfo
{
    public NodeId NodeId { get; }
    public NodeRole CurrentRole { get; }
}