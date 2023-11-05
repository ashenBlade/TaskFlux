using TaskFlux.Core;

namespace TaskFlux.Abstractions;

public interface IApplication : IReadOnlyApplication
{
    public new ITaskQueueManager TaskQueueManager { get; }
    IReadOnlyTaskQueueManager IReadOnlyApplication.TaskQueueManager => TaskQueueManager;
}