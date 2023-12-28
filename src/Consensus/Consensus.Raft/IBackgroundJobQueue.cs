namespace Consensus.Raft;

public interface IBackgroundJobQueue
{
    public void Accept(IBackgroundJob job, CancellationToken token);
}