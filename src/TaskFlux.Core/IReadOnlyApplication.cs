using TaskFlux.Core.Queue;

namespace TaskFlux.Core;

public interface IReadOnlyApplication
{
    public IReadOnlyTaskQueueManager TaskQueueManager { get; }
}