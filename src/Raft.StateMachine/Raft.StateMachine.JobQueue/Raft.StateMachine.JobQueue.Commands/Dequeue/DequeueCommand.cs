using JobQueue.Core;

namespace Raft.StateMachine.JobQueue.Commands.Dequeue;

public class DequeueCommand: ICommand
{
    private readonly DequeueRequest _request;

    public DequeueCommand(DequeueRequest request)
    {
        _request = request;
    }
    
    public JobQueueResponse Apply(IJobQueue jobQueue)
    {
        if (jobQueue.TryDequeue(out var key, out var payload))
        {
            return new DequeueJobQueueResponse(DequeueResponse.Ok(key, payload));
        }

        return new DequeueJobQueueResponse(DequeueResponse.Empty);
    }

    public void ApplyNoResponse(IJobQueue jobQueue)
    {
        jobQueue.TryDequeue(out _, out _);
    }
}