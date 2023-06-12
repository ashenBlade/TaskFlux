using Raft.Core;
using Serilog;

namespace Raft.JobQueue;

public class TaskJobQueue: IJobQueue
{
    private readonly ILogger _logger;

    public TaskJobQueue(ILogger logger)
    {
        _logger = logger;
    }
    public void EnqueueInfinite(Func<Task> job, CancellationToken token)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await job();
            }
            catch (Exception e)
            {
                _logger.Warning(e, "Во время исполнения задачи на фоновом потоке поймано необработанное исключение");
            }
        }, token);
    }
}