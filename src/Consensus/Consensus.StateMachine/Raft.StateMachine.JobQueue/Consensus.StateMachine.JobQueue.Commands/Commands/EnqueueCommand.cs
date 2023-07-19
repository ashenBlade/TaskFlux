using JobQueue.Core;
using TaskFlux.Requests;
using TaskFlux.Requests.Enqueue;
using TaskFlux.Requests.Requests.JobQueue.Enqueue;

namespace Consensus.StateMachine.JobQueue.Serializer.Commands;

public class EnqueueCommand: JobQueueCommand
{
    private readonly EnqueueRequest _request;

    public EnqueueCommand(EnqueueRequest request)
    {
        _request = request;
    }
    
    protected override IResponse Apply(IJobQueue jobQueue)
    {
        if (jobQueue.TryEnqueue(_request.Key, _request.Payload))
        {
            return EnqueueResponse.Ok;
        }
        
        return EnqueueResponse.Fail;
    }
    
    protected override void ApplyNoResponse(IJobQueue jobQueue)
    {
        jobQueue.TryEnqueue(_request.Key, _request.Payload);
    }
}