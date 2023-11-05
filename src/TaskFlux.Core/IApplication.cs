namespace TaskFlux.Core;

public interface IApplication : IReadOnlyApplication
{
    public new ITaskQueueManager TaskQueueManager { get; }
    IReadOnlyTaskQueueManager IReadOnlyApplication.TaskQueueManager => TaskQueueManager;
}