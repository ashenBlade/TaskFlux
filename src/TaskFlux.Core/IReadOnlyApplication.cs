using TaskFlux.Core.Queue;

namespace TaskFlux.Core;

public interface IReadOnlyApplication
{
    public INodeInfo NodeInfo { get; }
    public IApplicationInfo ApplicationInfo { get; }
    public IReadOnlyTaskQueueManager TaskQueueManager { get; }
}