namespace TaskFlux.Consensus.Tests.Stubs;

public class SingleRunBackgroundJobQueue : IBackgroundJobQueue
{
    private (IBackgroundJob Job, CancellationToken Token)? _pair;

    public void Accept(IBackgroundJob job, CancellationToken token)
    {
        _pair = ( job, token );
    }

    public void Run()
    {
        if (_pair is var (job, token))
        {
            job.Run(token);
        }
    }
}