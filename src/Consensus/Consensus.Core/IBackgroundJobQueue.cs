namespace Consensus.Core;

public interface IBackgroundJobQueue
{
    public void EnqueueInfinite(Func<Task> job, CancellationToken token);
}