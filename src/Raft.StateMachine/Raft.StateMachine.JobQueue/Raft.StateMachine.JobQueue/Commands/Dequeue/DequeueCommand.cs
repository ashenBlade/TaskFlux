using JobQueue.Core;
using Raft.StateMachine.JobQueue.Commands.Serializers;

namespace Raft.StateMachine.JobQueue.Commands.Dequeue;

public class DequeueCommand: ICommand
{
    private readonly DequeueRequest _request;

    public DequeueCommand(DequeueRequest request)
    {
        _request = request;
    }
    
    public IJobQueueResponse Apply(IJobQueue jobQueue)
    {
        if (jobQueue.TryDequeue(out var key, out var payload))
        {
            return DequeueResponse.Ok(key, payload);
        }

        return DequeueResponse.Empty;
    }

    public void ApplyNoResponse(IJobQueue jobQueue)
    {
        jobQueue.TryDequeue(out _, out _);
    }
}