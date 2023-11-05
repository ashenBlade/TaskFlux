using TaskFlux.Abstractions;

namespace TaskFlux.Core;

public class TaskFluxApplication : IApplication
{
    public TaskFluxApplication(
        INodeInfo nodeInfo,
        IClusterInfo clusterInfo,
        IApplicationInfo applicationInfo,
        ITaskQueueManager taskQueueManager)
    {
        ArgumentNullException.ThrowIfNull(nodeInfo);
        ArgumentNullException.ThrowIfNull(clusterInfo);
        ArgumentNullException.ThrowIfNull(applicationInfo);
        ArgumentNullException.ThrowIfNull(taskQueueManager);

        NodeInfo = nodeInfo;
        ClusterInfo = clusterInfo;
        ApplicationInfo = applicationInfo;
        TaskQueueManager = taskQueueManager;
    }

    public INodeInfo NodeInfo { get; }
    public IClusterInfo ClusterInfo { get; }
    public IApplicationInfo ApplicationInfo { get; }
    public ITaskQueueManager TaskQueueManager { get; }
}