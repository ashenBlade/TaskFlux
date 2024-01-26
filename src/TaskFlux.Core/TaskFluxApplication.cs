using TaskFlux.Core.Queue;

namespace TaskFlux.Core;

public class TaskFluxApplication : IApplication
{
    public TaskFluxApplication(ITaskQueueManager taskQueueManager)
    {
        ArgumentNullException.ThrowIfNull(taskQueueManager);

        TaskQueueManager = taskQueueManager;
    }

    public ITaskQueueManager TaskQueueManager { get; }
}