namespace Raft.StateMachine.JobQueue.Commands.GetCount;

public record GetCountRequest(): IDefaultRequest
{
    public static readonly GetCountRequest Instance = new();
    
    public RequestType Type => RequestType.GetCountRequest;
    public void Accept(IDefaultRequestVisitor visitor)
    {
        visitor.Visit(this);
    }
}