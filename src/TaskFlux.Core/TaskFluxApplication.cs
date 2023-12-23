using TaskFlux.Core.Queue;

namespace TaskFlux.Core;

public class TaskFluxApplication : IApplication
{
    public TaskFluxApplication(
        INodeInfo nodeInfo,
        IApplicationInfo applicationInfo,
        ITaskQueueManager taskQueueManager)
    {
        ArgumentNullException.ThrowIfNull(nodeInfo);
        ArgumentNullException.ThrowIfNull(applicationInfo);
        ArgumentNullException.ThrowIfNull(taskQueueManager);

        NodeInfo = nodeInfo;
        ApplicationInfo = applicationInfo;
        TaskQueueManager = taskQueueManager;
    }

    public INodeInfo NodeInfo { get; }
    public IApplicationInfo ApplicationInfo { get; }
    public ITaskQueueManager TaskQueueManager { get; }
}