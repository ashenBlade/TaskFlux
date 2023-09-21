namespace Consensus.Raft.Tests.Stubs;

public class SingleRunBackgroundJobQueue : IBackgroundJobQueue
{
    public (Func<Task> Job, CancellationToken Token)? Job { get; set; }

    public void RunInfinite(Func<Task> job, CancellationToken token)
    {
        Job = ( job, CancellationToken.None );
    }

    public void Run()
    {
        if (Job is not {Job: { } job, Token: var token})
        {
            throw new ArgumentNullException(nameof(Job), "Задача не была зарегистрирована");
        }


        job().Wait();
    }
}