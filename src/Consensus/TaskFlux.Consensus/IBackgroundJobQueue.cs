namespace TaskFlux.Consensus;

public interface IBackgroundJobQueue
{
    public void Accept(IBackgroundJob job, CancellationToken token);
}