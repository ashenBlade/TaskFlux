using TaskFlux.Commands;
using TaskFlux.Core;

namespace TaskFlux.Host.Infrastructure;

public class CommandContext: ICommandContext
{
    public CommandContext(INode node, INodeInfo nodeInfo, IApplicationInfo applicationInfo, IClusterInfo clusterInfo)
    {
        Node = node;
        NodeInfo = nodeInfo;
        ApplicationInfo = applicationInfo;
        ClusterInfo = clusterInfo;
    }

    public INode Node { get; }
    public INodeInfo NodeInfo { get; }
    public IApplicationInfo ApplicationInfo { get; }
    public IClusterInfo ClusterInfo { get; }
}