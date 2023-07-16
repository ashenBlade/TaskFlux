using JobQueue.Core;
using Raft.StateMachine.JobQueue.Commands.Dequeue;

namespace Raft.StateMachine.JobQueue.Commands.GetCount;

public class GetCountCommand: IJobQueueCommand
{
    private readonly GetCountRequest _request;

    public GetCountCommand(GetCountRequest request)
    {
        _request = request;
    }
    
    public IJobQueueResponse Apply(IJobQueue jobQueue)
    {
        return new GetCountResponse(jobQueue.Count);
    }

    public void ApplyNoResponse(IJobQueue jobQueue)
    { }
}