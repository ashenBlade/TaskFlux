namespace Consensus.Core;

public interface IJobQueue
{
    public void EnqueueInfinite(Func<Task> job, CancellationToken token);
}