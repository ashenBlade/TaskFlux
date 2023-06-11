using Raft.Core;

namespace Raft.JobQueue;

public class TaskJobQueue: IJobQueue
{
    public void EnqueueInfinite(Func<Task> job, CancellationToken token)
    {
        _ = Task.Run(async () =>
        {
            while (token.IsCancellationRequested is false)
            {
                await job();
            }
        }, token);
    }
}