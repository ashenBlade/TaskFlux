using TaskFlux.Core;

namespace TaskFlux.Node;

public class NodeInfo : INodeInfo
{
    public NodeInfo(NodeId nodeId, NodeRole startRole)
    {
        NodeId = nodeId;
        CurrentRole = startRole;
    }

    public NodeId NodeId { get; }
    public NodeRole CurrentRole { get; set; }

    public override string ToString()
    {
        return$"PocoNodeInfo(NodeId = {NodeId.Value}, CurrentRole = {CurrentRole})";
    }
}