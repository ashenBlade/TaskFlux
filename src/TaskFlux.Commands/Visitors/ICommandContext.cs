using TaskFlux.Core;

namespace TaskFlux.Commands.Visitors;

public interface ICommandContext
{
    public INode Node { get; }
    public INodeInfo NodeInfo { get; }
    public IApplicationInfo ApplicationInfo { get; }
    public IClusterInfo ClusterInfo { get; }
}