namespace Raft.StateMachine.JobQueue.Commands.GetCount;

public record GetCountResponse(int Count): IDefaultResponse
{
    public static readonly GetCountResponse Empty = new(0);
    public ResponseType Type => ResponseType.GetCount;
    public void Accept(IDefaultResponseVisitor visitor)
    {
        visitor.Visit(this);
    }
}
