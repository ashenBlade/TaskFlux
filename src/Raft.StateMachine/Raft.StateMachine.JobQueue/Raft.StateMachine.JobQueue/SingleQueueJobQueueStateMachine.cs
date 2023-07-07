using JobQueue.Core;
using Raft.StateMachine.JobQueue.Requests;

namespace Raft.StateMachine.JobQueue;

public class SingleQueueJobQueueStateMachine: IJobQueueStateMachine
{
    private readonly IJobQueue _queue;

    public SingleQueueJobQueueStateMachine(IJobQueue queue)
    {
        _queue = queue;
    }
    
    public DequeueResponse Apply(DequeueRequest request)
    {
        if (_queue.TryDequeue(out var key, out var payload))
        {
            return DequeueResponse.Ok(key, payload);
        }

        return DequeueResponse.Empty;
    }

    public EnqueueResponse Apply(EnqueueRequest request)
    {
        if (_queue.TryEnqueue(request.Key, request.Payload))
        {
            return EnqueueResponse.Ok;
        }
        
        return EnqueueResponse.Fail;
    }

    public GetCountResponse Apply(GetCountRequest request)
    {
        return _queue.Count is 0
                   ? GetCountResponse.Empty
                   : new GetCountResponse(_queue.Count);
    }
}