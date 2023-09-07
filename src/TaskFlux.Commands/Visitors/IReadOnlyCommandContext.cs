using TaskFlux.Core;

namespace TaskFlux.Commands.Visitors;

public interface IReadOnlyCommandContext
{
    public IReadOnlyNode Node { get; }
    public INodeInfo NodeInfo { get; }
    public IApplicationInfo ApplicationInfo { get; }
    public IClusterInfo ClusterInfo { get; }
}