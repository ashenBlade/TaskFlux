namespace Consensus.Raft;

public interface IBackgroundJobQueue
{
    public void RunInfinite(Func<Task> job, CancellationToken token);
}