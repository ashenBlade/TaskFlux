namespace Consensus.Core.Tests;

public class SingleRunJobQueue : IJobQueue
{
    public (Func<Task> Job, CancellationToken Token)? Job { get; set; }
    public void EnqueueInfinite(Func<Task> job, CancellationToken token)
    {
        Job = (job, token);
    }

    public async Task Run()
    {
        if (Job is not {Job: {} job})
        {
            throw new ArgumentNullException(nameof(Job), "Задача не была зарегистрирована");
        }
            

        await job();
    }
}