using Consensus.Core;
using Serilog;

namespace Consensus.JobQueue;

public class TaskBackgroundJobQueue: IBackgroundJobQueue
{
    private readonly ILogger _logger;

    public TaskBackgroundJobQueue(ILogger logger)
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