using JobQueue.Core;
using TaskFlux.Requests;
using TaskFlux.Requests.Dequeue;

namespace Consensus.StateMachine.JobQueue.Serializer.Commands;

public class DequeueCommand: JobQueueCommand
{
    private readonly DequeueRequest _request;

    public DequeueCommand(DequeueRequest request)
    {
        _request = request;
    }
    
    protected override IResponse Apply(IJobQueue jobQueue)
    {
        if (jobQueue.TryDequeue(out var key, out var payload))
        {
            return DequeueResponse.Ok(key, payload);
        }
        
        return DequeueResponse.Empty;
    }

    protected override void ApplyNoResponse(IJobQueue jobQueue)
    {
        jobQueue.TryDequeue(out _, out _);
    }
}