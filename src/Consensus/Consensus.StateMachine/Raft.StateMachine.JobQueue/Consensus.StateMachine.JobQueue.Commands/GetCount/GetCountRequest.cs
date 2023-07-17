namespace Consensus.StateMachine.JobQueue.Commands.GetCount;

public record GetCountRequest(): IJobQueueRequest
{
    public static readonly GetCountRequest Instance = new();
    
    public RequestType Type => RequestType.GetCountRequest;
    public void Accept(IJobQueueRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}