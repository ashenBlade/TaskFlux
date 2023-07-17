using JobQueue.Core;

namespace Consensus.StateMachine.JobQueue.Commands.Enqueue;

public class EnqueueCommand: IJobQueueCommand
{
    private readonly EnqueueRequest _request;

    public EnqueueCommand(EnqueueRequest request)
    {
        _request = request;
    }
    
    public IJobQueueResponse Apply(IJobQueue jobQueue)
    {
        if (jobQueue.TryEnqueue(_request.Key, _request.Payload))
        {
            return EnqueueResponse.Ok;
        }

        return EnqueueResponse.Fail;
    }

    public void ApplyNoResponse(IJobQueue jobQueue)
    {
        jobQueue.TryEnqueue(_request.Key, _request.Payload);
    }
}