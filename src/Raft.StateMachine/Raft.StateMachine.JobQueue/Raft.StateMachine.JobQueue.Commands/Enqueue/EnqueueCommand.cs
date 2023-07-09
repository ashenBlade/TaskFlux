using JobQueue.Core;
using Raft.StateMachine.JobQueue.Commands.Dequeue;

namespace Raft.StateMachine.JobQueue.Commands.Enqueue;

public class EnqueueCommand: ICommand
{
    private readonly EnqueueRequest _request;

    public EnqueueCommand(EnqueueRequest request)
    {
        _request = request;
    }
    
    public JobQueueResponse Apply(IJobQueue jobQueue)
    {
        if (jobQueue.TryEnqueue(_request.Key, _request.Payload))
        {
            return new EnqueueJobQueueResponse(EnqueueResponse.Ok);
        }

        return new EnqueueJobQueueResponse(EnqueueResponse.Fail);
    }

    public void ApplyNoResponse(IJobQueue jobQueue)
    {
        jobQueue.TryEnqueue(_request.Key, _request.Payload);
    }
}